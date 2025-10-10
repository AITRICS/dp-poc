"""Topic pattern matching utilities for wildcard subscriptions."""

import re
from re import Pattern


def compile_topic_pattern(pattern: str) -> Pattern[str]:
    """
    Compile a topic pattern into a regex pattern.

    Supports wildcards:
    - * matches any single level (e.g., data.* matches data.pipeline but not data.pipeline.ingestion)
    - ** matches multiple levels (e.g., data.** matches data.pipeline.ingestion)

    Args:
        pattern: Topic pattern with wildcards.

    Returns:
        Compiled regex pattern.

    Examples:
        >>> compile_topic_pattern("data.*")
        >>> compile_topic_pattern("data.**")
        >>> compile_topic_pattern("data.*.ingestion")
    """
    # Escape special regex characters except * and .
    escaped = re.escape(pattern)

    # Replace escaped wildcards with regex equivalents
    # ** matches any characters including dots (multiple levels)
    regex_pattern = escaped.replace(r"\*\*", r".*")
    # * matches any characters except dots (single level)
    regex_pattern = regex_pattern.replace(r"\*", r"[^.]+")

    # Anchor the pattern to match the entire string
    regex_pattern = f"^{regex_pattern}$"

    return re.compile(regex_pattern)


def match_topic_pattern(pattern: str, topic: str) -> bool:
    """
    Check if a topic matches a pattern.

    Supports wildcards:
    - * matches any single level
    - ** matches multiple levels

    Args:
        pattern: Topic pattern with wildcards.
        topic: Actual topic name to match against.

    Returns:
        True if the topic matches the pattern, False otherwise.

    Examples:
        >>> match_topic_pattern("data.*", "data.pipeline")
        True
        >>> match_topic_pattern("data.*", "data.pipeline.ingestion")
        False
        >>> match_topic_pattern("data.**", "data.pipeline.ingestion")
        True
        >>> match_topic_pattern("data.*.ingestion", "data.pipeline.ingestion")
        True
    """
    regex = compile_topic_pattern(pattern)
    return regex.match(topic) is not None


def get_matching_topics(pattern: str, available_topics: set[str]) -> set[str]:
    """
    Get all topics that match the given pattern.

    Args:
        pattern: Topic pattern with wildcards.
        available_topics: Set of available topic names.

    Returns:
        Set of topic names that match the pattern.

    Examples:
        >>> topics = {"data.pipeline", "data.warehouse", "data.pipeline.ingestion"}
        >>> get_matching_topics("data.*", topics)
        {'data.pipeline', 'data.warehouse'}
        >>> get_matching_topics("data.**", topics)
        {'data.pipeline', 'data.warehouse', 'data.pipeline.ingestion'}
    """
    regex = compile_topic_pattern(pattern)
    return {topic for topic in available_topics if regex.match(topic)}
