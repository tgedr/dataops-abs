import pytest

from tgedr_dataops_abs.etl4gh import Etl4GH


class MyEtl4GH(Etl4GH):
    def __init__(self, result):
        super().__init__()
        self.result = result
        self.calls: list[str] = []

    def extract(self) -> None:
        self.calls.append("extract")

    def validate_extract(self) -> None:
        self.calls.append("validate_extract")

    def transform(self) -> None:
        self.calls.append("transform")

    def validate_transform(self) -> None:
        self.calls.append("validate_transform")

    def load(self) -> str:
        self.calls.append("load")
        return self.result


def test_etl4gh_run_writes_result_to_github_output(tmp_path, monkeypatch):
    output_file = tmp_path / "github_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

    etl = MyEtl4GH("true")

    result = etl.run()

    assert result is None
    assert output_file.read_text(encoding="utf-8") == "result=true\n"
    assert etl.calls == ["extract", "validate_extract", "transform", "validate_transform", "load"]


def test_etl4gh_is_still_abstract() -> None:
    with pytest.raises(TypeError):
        Etl4GH()
