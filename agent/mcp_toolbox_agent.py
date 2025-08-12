#!/usr/bin/env python3
import json
import logging
import os
import sys
import asyncio
from aiohttp.client_exceptions import ServerDisconnectedError
from dotenv import load_dotenv
from sqlalchemy import JSON
from toolbox_core import ToolboxClient
from google.genai.types import Content, Part
from google.adk.agents import Agent
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner
from rich.console import Console
from rich.progress import track

console = Console()

# Add the parent directory to Python path to import utils and tools
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import logger
from tools.query_refiner import query_refiner

load_dotenv()
logger = logging.getLogger(__name__)

MCP_URL = os.getenv("MCP_TOOLBOX_URL", "http://127.0.0.1:5000")
APP_NAME = USER_ID = SESSION_ID = "dynamic_text2sql_agent"

def get_llm():
    api_key = os.getenv("GOOGLE_API_KEY")
    if api_key:
        from google.adk.models import Gemini
        return Gemini(model_name="gemini-2.5-pro", api_key=api_key)
    logger.info("GOOGLE_API_KEY not set — using fallback model string.")
    return "gemini-2.5-flash"

async def interaction_loop(runner: Runner):
    logger.info("\nAgent ready. Type your question or 'exit' to quit.")
    while True:
        question = await asyncio.to_thread(input, "You> ")
        if question.strip().lower() == "exit":
            logger.info("Goodbye!")
            break

        msg = Content(role="user", parts=[Part(text=question)])
        final_text = None

        async for ev in runner.run_async(new_message=msg, user_id=USER_ID, session_id=SESSION_ID):
            for fc in ev.get_function_calls():
                logger.info(f"Tool call: {fc.name} args={fc.args}")

            for fr in ev.get_function_responses():
                logger.info(f"Response from {fr.name}:\n {fr.response}")
                
            if ev.content and ev.is_final_response():
                final_text = ev.content.parts[0].text

        logger.info(f"Assistant: {final_text or '(no final text)'}")

async def build_runner_and_client():
    client = ToolboxClient(MCP_URL)
    await client.__aenter__()
    toolbox_tools = await client.load_toolset()

    session = InMemorySessionService()
    await session.create_session(app_name=APP_NAME, user_id=USER_ID, session_id=SESSION_ID, state={})
    
    # Combine custom tools with toolbox tools
    all_tools = [query_refiner]
    if isinstance(toolbox_tools, list):
        all_tools.extend(toolbox_tools)
    else:
        all_tools.append(toolbox_tools)
    
    agent = Agent(
        name=APP_NAME,
        model=get_llm(),
        tools=all_tools,
        instruction=(
            """
            # Introduction
            You are a multi-DB text-to-SQL agent. Use only these tools:
            <db_key>_list_tables, <db_key>_describe_table, <db_key>_execute_query, query_refiner

            # Steps
            1. If you can't figure out which DB the question is about, ask the user to clarify. 
            2. Identify the database type from the or context.
            3. Always inspect schema before executing querying.
            4. First get all correct table names and column names as per the schema, then use those to construct your SQL queries. 
            5. Use the `query_refiner` tool to wrap table and column names with appropriate delimiters based on the database type.
            6. The text with '@' is the target database key, e.g., '@superheroes', '@employee'. Use it to determine the database.
            """
        )
    )
    runner = Runner(app_name=APP_NAME, agent=agent, session_service=session)
    return runner, client

async def main():
    retry = 0
    client = None
    try:
        while True:
            try:
                runner, client = await build_runner_and_client()
                logger.info("Agent initialized and connected to MCP Toolbox.")
                await interaction_loop(runner)
                break
            except ServerDisconnectedError:
                retry += 1
                logger.error(f"Disconnected—retry #{retry} in 1s...")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break
    finally:
        if client:
            try:
                await client.close()
            except Exception as close_error:
                logger.warning(f"Warning: Error closing client: {close_error}")

if __name__ == "__main__":
    asyncio.run(main())
