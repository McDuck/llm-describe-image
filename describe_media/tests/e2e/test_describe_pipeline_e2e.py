import os
import shutil
import subprocess
import sys
from pathlib import Path

from PIL import Image


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures"


def run_cli(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    full_env = os.environ.copy()
    # Keep fixtures offline and independent of a developer's .env settings.
    full_env["REVERSE_GEOCODE_GPS"] = "false"
    full_env.update(env)
    return subprocess.run(
        [sys.executable, "-m", "describe_media.describe_media", *args],
        cwd=REPO_ROOT,
        env=full_env,
        text=True,
        capture_output=True,
        check=False,
    )


def copy_tree(src: Path, dest: Path) -> None:
    shutil.copytree(src, dest)


def normalized_text(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n").rstrip("\n")


def assert_expected_text_tree(actual: Path, expected: Path) -> None:
    expected_files = sorted(path for path in expected.rglob("*") if path.is_file())
    actual_text_files = sorted(
        path for path in actual.rglob("*") if path.is_file() and path.suffix in {".txt"}
    )

    expected_rel = [path.relative_to(expected) for path in expected_files]
    actual_rel = [path.relative_to(actual) for path in actual_text_files]

    assert actual_rel == expected_rel, f"Actual text outputs do not match expected set: {actual_rel} != {expected_rel}"

    for rel_path in expected_rel:
        actual_file = actual / rel_path
        expected_file = expected / rel_path
        assert normalized_text(actual_file) == normalized_text(expected_file), f"Content mismatch for {rel_path}"


def test_describe_pipeline_matches_expected_output_fixture(tmp_path: Path) -> None:
    scenario_root = FIXTURES_ROOT / "describe"
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    expected_dir = scenario_root / "expected"

    copy_tree(scenario_root / "input", input_dir)

    result = run_cli(
        ["describe", str(input_dir), str(output_dir)],
        {
            "BACKEND": "mock",
            "MOCK_LLM_RESPONSE_TEMPLATE": "caption for {image_name}",
        },
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert_expected_text_tree(output_dir, expected_dir)

    for resized_name in ["mona_lisa.resized.jpg", "girl_with_a_pearl_earring.resized.jpg"]:
        resized_file = output_dir / "album" / resized_name
        assert resized_file.exists()
        with Image.open(resized_file) as image:
            assert image.width <= 720
            assert image.height <= 720


def test_describe_pipeline_retry_flow_matches_expected_fixtures(tmp_path: Path) -> None:
    scenario_root = FIXTURES_ROOT / "retry"
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    expected_error_dir = scenario_root / "expected_error"
    expected_retry_dir = scenario_root / "expected_retry"

    copy_tree(scenario_root / "input", input_dir)

    first = run_cli(
        ["describe", str(input_dir), str(output_dir)],
        {
            "BACKEND": "mock",
            "MOCK_LLM_RESPONSE_ERROR": "synthetic failure",
        },
    )
    assert first.returncode == 0, first.stderr + first.stdout
    assert_expected_text_tree(output_dir, expected_error_dir)

    second = run_cli(
        ["describe", str(input_dir), str(output_dir)],
        {
            "BACKEND": "mock",
            "MOCK_LLM_RESPONSE_TEMPLATE": "should not run",
        },
    )
    assert second.returncode == 0, second.stderr + second.stdout
    assert_expected_text_tree(output_dir, expected_error_dir)

    third = run_cli(
        ["describe", str(input_dir), str(output_dir), "--retry-failed"],
        {
            "BACKEND": "mock",
            "MOCK_LLM_RESPONSE_TEMPLATE": "recovered description",
            "OUTPUT_FORMAT": "{content}",
        },
    )
    assert third.returncode == 0, third.stderr + third.stdout
    assert_expected_text_tree(output_dir, expected_retry_dir)
