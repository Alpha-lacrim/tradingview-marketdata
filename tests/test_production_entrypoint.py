import os

from marketdata.production import command


def test_file_secrets_and_bounded_api_command(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "environ", dict(os.environ))
    monkeypatch.setenv("DATABASE_URL_FILE", str(tmp_path / "dsn"))
    monkeypatch.setenv("API_KEY_FILE", str(tmp_path / "token"))
    (tmp_path / "dsn").write_text("postgresql://reader:disposable@localhost/qualification_test")
    (tmp_path / "token").write_text("x" * 32)
    arguments = command("api")
    assert "--no-access-log" in arguments
    assert arguments[arguments.index("--ws-max-queue") + 1] == "16"
    assert "x" * 32 not in " ".join(arguments)
    assert "disposable" not in " ".join(arguments)
