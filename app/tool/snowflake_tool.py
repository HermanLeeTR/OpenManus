from typing import Any, Dict, Optional

from app.agent.org_authority import OrgAuthorityAgent
from app.agent.person_authority import PersonAuthorityAgent
from app.config import config
from app.logger import logger
from app.tool.base import BaseTool


class SnowflakeTool(BaseTool):
    """Tool for interacting with Snowflake database using natural language."""

    name: str = "snowflake"
    description: str = "Query Snowflake database using natural language"
    org_agent: Optional[OrgAuthorityAgent] = None
    person_agent: Optional[PersonAuthorityAgent] = None

    def __init__(self):
        super().__init__()
        logger.info("Initializing SnowflakeTool")
        self.org_agent = OrgAuthorityAgent(config=config)
        self.person_agent = PersonAuthorityAgent(config=config)

    async def execute(self, query: str, agent_type: str = "org") -> Any:
        """Execute a natural language query on Snowflake.

        Args:
            query: The natural language query to execute
            agent_type: The type of agent to use ("org" or "person")
        """
        if agent_type == "org":
            if not self.org_agent:
                logger.info("Creating new OrgAuthorityAgent instance")
                self.org_agent = OrgAuthorityAgent(config=config)
            agent = self.org_agent
        elif agent_type == "person":
            if not self.person_agent:
                logger.info("Creating new PersonAuthorityAgent instance")
                self.person_agent = PersonAuthorityAgent(config=config)
            agent = self.person_agent
        else:
            raise ValueError(
                f"Invalid agent type: {agent_type}. Must be 'org' or 'person'"
            )

        try:
            logger.info(f"Executing Snowflake query using {agent_type} agent: {query}")
            result = await agent.process_query(query)
            logger.info("Snowflake query executed successfully")
            return result
        except Exception as e:
            logger.error(f"Error executing Snowflake query: {str(e)}")
            raise

    def to_param(self) -> Dict[str, Any]:
        """Convert tool to parameter format."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language query to execute on Snowflake",
                        },
                        "agent_type": {
                            "type": "string",
                            "description": "Type of agent to use ('org' or 'person')",
                            "enum": ["org", "person"],
                            "default": "org",
                        },
                    },
                    "required": ["query"],
                },
            },
        }
