import unittest
import os
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio

from agent.mcp_toolbox_agent import get_llm, build_runner_and_client

class TestMcpToolboxAgent(unittest.IsolatedAsyncioTestCase):

    @patch.dict(os.environ, {"GOOGLE_API_KEY": "test_key"})
    def test_get_llm_with_api_key(self):
        """
        Tests that get_llm returns a Gemini model when GOOGLE_API_KEY is set.
        """
        with patch('google.adk.models.Gemini') as mock_gemini:
            llm = get_llm()
            mock_gemini.assert_called_once_with(model_name="gemini-2.5-pro", api_key="test_key")
            self.assertEqual(llm, mock_gemini.return_value)

    @patch.dict(os.environ, {}, clear=True)
    def test_get_llm_without_api_key(self):
        """
        Tests that get_llm returns the fallback model string when GOOGLE_API_KEY is not set.
        """
        llm = get_llm()
        self.assertEqual(llm, "gemini-2.5-flash")

    @patch('agent.mcp_toolbox_agent.ToolboxClient')
    @patch('agent.mcp_toolbox_agent.InMemorySessionService')
    @patch('agent.mcp_toolbox_agent.Agent')
    @patch('agent.mcp_toolbox_agent.Runner')
    @patch('agent.mcp_toolbox_agent.query_refiner')
    async def test_build_runner_and_client(self, mock_query_refiner, mock_runner, mock_agent, mock_session_service, mock_toolbox_client):
        """
        Tests the construction of the Runner and ToolboxClient, mocking external dependencies.
        """
        # Mock the async methods and properties
        mock_toolbox_client.return_value.__aenter__ = AsyncMock()
        mock_toolbox_client.return_value.load_toolset = AsyncMock(return_value=[])
        mock_session_service.return_value.create_session = AsyncMock()

        # Run the function
        runner, client = await build_runner_and_client()

        # Assertions
        mock_toolbox_client.assert_called_once_with("http://127.0.0.1:5000")
        mock_session_service.assert_called_once()
        mock_agent.assert_called_once()
        mock_runner.assert_called_once()

        # Check that the agent was created with the correct tools
        agent_args, agent_kwargs = mock_agent.call_args
        self.assertIn('tools', agent_kwargs)
        self.assertIn(mock_query_refiner, agent_kwargs['tools'])

    @patch('agent.mcp_toolbox_agent.ToolboxClient')
    @patch('agent.mcp_toolbox_agent.InMemorySessionService')
    @patch('agent.mcp_toolbox_agent.Agent')
    @patch('agent.mcp_toolbox_agent.Runner')
    async def test_agent_instruction(self, mock_runner, mock_agent, mock_session_service, mock_toolbox_client):
        """
        Tests that the agent is initialized with the correct instruction prompt.
        """
        mock_toolbox_client.return_value.__aenter__ = AsyncMock()
        mock_toolbox_client.return_value.load_toolset = AsyncMock(return_value=[])
        mock_session_service.return_value.create_session = AsyncMock()

        await build_runner_and_client()

        agent_args, agent_kwargs = mock_agent.call_args
        self.assertIn('instruction', agent_kwargs)
        self.assertIn("You are a multi-DB text-to-SQL agent.", agent_kwargs['instruction'])


if __name__ == '__main__':
    unittest.main()
