from pathlib import Path

from dsgrid.utils.scratch_dir_context import ScratchDirContext


def add_content(context: ScratchDirContext) -> tuple[Path, list[Path]]:
    """Add files and directories to the scratch directory."""
    files = []
    for _ in range(2):
        filename = context.get_temp_filename()
        assert not filename.exists()
        filename.touch()
        files.append(filename)

    directory = context.scratch_dir / "my_dir"
    directory.mkdir()
    filename = directory / "file.txt"
    filename.touch()
    context.add_tracked_path(directory)
    return directory, files


def test_scratch_dir_context(tmp_path):
    """Test nominal usage."""
    scratch_dir = tmp_path / "scratch_dir"
    with ScratchDirContext(scratch_dir) as context:
        assert context.scratch_dir == scratch_dir
        directory, files = add_content(context)
        assert sorted(context.list_tracked_paths()) == sorted([directory] + files)

    assert not scratch_dir.exists()


def test_scratch_dir_manual_deletion(tmp_path):
    """Verify functionality when the user manually deleted a tracked file."""
    scratch_dir = tmp_path / "scratch_dir"
    with ScratchDirContext(scratch_dir) as context:
        _, files = add_content(context)
        files[0].unlink()

    assert not scratch_dir.exists()


def test_scratch_dir_add_extra_file(tmp_path):
    """Verify that extra files added to the scratch directory are not deleted."""
    scratch_dir = tmp_path / "scratch_dir"
    extra_file = scratch_dir / "extra.txt"
    with ScratchDirContext(scratch_dir) as context:
        add_content(context)
        extra_file.touch()

    files = list(scratch_dir.iterdir())
    assert len(files) == 1
    assert files[0] == extra_file
