import pytest

from mtbls.domain.domain_services.modifier.base_modifier import BaseModifier


class TestBaseModifier(BaseModifier): ...


class TestGetListString:
    @pytest.mark.parametrize(
        "list_item",
        [
            None,
            "",
            [],
        ],
    )
    def test_get_list_string_01_empty(self, list_item):
        modifier = TestBaseModifier()
        assert modifier.get_list_string(list_item) == ""

    @pytest.mark.parametrize("limit", [0, -1, -10])
    def test_get_list_string_01_invalid_limit(self, limit: int):
        modifier = TestBaseModifier()
        assert modifier.get_list_string(["1", "2"], limit) == ""

    less_items = [
        [str(x) for x in range(1)],
        [str(x) for x in range(5)],
        [str(x) for x in range(10)],
    ]

    @pytest.mark.parametrize("list_item", less_items)
    def test_get_list_string_02_less_items(self, list_item: list[str]):
        modifier = TestBaseModifier()
        expected_list = list_item.copy()
        expected_list.sort()
        expected = ", ".join([str(x) for x in expected_list])
        assert modifier.get_list_string(list_item) == expected

    more_items = [
        [str(x) for x in range(11)],
        [str(x) for x in range(15)],
        [str(x) for x in range(20)],
    ]

    @pytest.mark.parametrize("list_item", more_items)
    def test_get_list_string_03_more_items(self, list_item: list[str]):
        modifier = TestBaseModifier()
        limit = 5
        expected_list = list_item.copy()
        expected_list.sort()
        expected = (
            ", ".join([str(x) for x in expected_list[:limit]])
            + " ... "
            + f"(total: {len(expected_list)})"
        )
        actual = modifier.get_list_string(list_item, limit=limit)
        assert actual == expected
