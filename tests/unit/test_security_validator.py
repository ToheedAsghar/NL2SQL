"""Tests for SecurityValidator (nl2sql_agents/agents/validator/security_validator.py).

This validator runs entirely without LLM — pure regex-based SQL injection detection.
"""

from __future__ import annotations

import pytest

from nl2sql_agents.agents.validator.security_validator import SecurityValidator


@pytest.fixture
def validator() -> SecurityValidator:
    return SecurityValidator()


class TestSecurityValidatorPassing:
    """Queries that should pass security checks."""

    @pytest.mark.asyncio
    async def test_simple_select(self, validator):
        result = await validator.check("SELECT * FROM singer")
        assert result.passed is True
        assert result.score == 1.0

    @pytest.mark.asyncio
    async def test_select_with_where(self, validator):
        result = await validator.check("SELECT Name FROM singer WHERE Country = 'France'")
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_select_with_join(self, validator):
        sql = """
        SELECT s.Name, c.concert_Name
        FROM singer s
        JOIN singer_in_concert sc ON s.Singer_ID = sc.Singer_ID
        JOIN concert c ON sc.concert_ID = c.concert_ID
        """
        result = await validator.check(sql)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_with_cte(self, validator):
        sql = "WITH cte AS (SELECT 1 AS n) SELECT * FROM cte"
        result = await validator.check(sql)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_select_with_subquery(self, validator):
        sql = "SELECT * FROM singer WHERE Singer_ID IN (SELECT Singer_ID FROM singer_in_concert)"
        result = await validator.check(sql)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_aggregate_query(self, validator):
        sql = "SELECT Country, COUNT(*) FROM singer GROUP BY Country HAVING COUNT(*) > 1"
        result = await validator.check(sql)
        assert result.passed is True


class TestSecurityValidatorBlocking:
    """Queries that should be blocked."""

    @pytest.mark.asyncio
    async def test_blocks_insert(self, validator):
        result = await validator.check("INSERT INTO singer VALUES (1, 'Test', 'US', 25)")
        assert result.passed is False
        assert result.score == 0.0

    @pytest.mark.asyncio
    async def test_blocks_update(self, validator):
        result = await validator.check("UPDATE singer SET Name = 'Hacked'")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_delete(self, validator):
        result = await validator.check("DELETE FROM singer WHERE Singer_ID = 1")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_drop(self, validator):
        result = await validator.check("DROP TABLE singer")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_alter(self, validator):
        result = await validator.check("ALTER TABLE singer ADD COLUMN email TEXT")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_truncate(self, validator):
        result = await validator.check("TRUNCATE TABLE singer")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_create(self, validator):
        result = await validator.check("CREATE TABLE evil (id INT)")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_stacked_queries(self, validator):
        result = await validator.check("SELECT 1; INSERT INTO singer VALUES (99, 'x', 'x', 0)")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_inline_comment_injection(self, validator):
        result = await validator.check("SELECT * FROM singer -- WHERE 1=1")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_exec(self, validator):
        result = await validator.check("EXEC sp_executesql N'SELECT 1'")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_blocks_non_select_start(self, validator):
        result = await validator.check("CALL some_procedure()")
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_check_name_is_security(self, validator):
        result = await validator.check("SELECT 1")
        assert result.check_name.lower() == "security"
