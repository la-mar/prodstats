import util.deco as deco


class TestSafeConvert:
    def test_none_on_error(self):
        @deco.safe_convert
        def try_int(s: str) -> int:
            return int(s)

        actual = try_int("a")
        assert actual is None

    def test_return_inputs_on_error(self):
        @deco.safe_convert(return_none_on_error=False)
        def try_int(s: str) -> int:
            return int(s)

        actual = try_int("a")
        assert actual == (("a",), {})
