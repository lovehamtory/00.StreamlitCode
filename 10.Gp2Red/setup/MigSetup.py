import ctypes
import subprocess
import sys
from pathlib import Path


def project_root() -> Path:
    executable = Path(sys.executable).resolve() if getattr(sys, "frozen", False) else Path(__file__).resolve()
    return executable.parent.parent


def show_message(text: str, title: str, level: int) -> None:
    ctypes.windll.user32.MessageBoxW(None, text, title, level)


def select_wheel_folder() -> str | None:
    command = [
        "powershell.exe",
        "-NoProfile",
        "-STA",
        "-Command",
        "Add-Type -AssemblyName System.Windows.Forms; $dialog = New-Object System.Windows.Forms.FolderBrowserDialog; $dialog.Description = '서버에 복사한 whl 폴더 선택'; if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) { [Console]::Write($dialog.SelectedPath) }",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return result.stdout.strip() or None


def run_installer(installer: Path, root: Path, wheel_folder: str | None = None) -> int:
    command = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(installer)]
    if wheel_folder:
        command.extend(["-WheelFolder", wheel_folder])
    return subprocess.run(command, cwd=root, check=False).returncode


def main() -> int:
    root = project_root()
    installer = root / "setup" / "install.ps1"
    if not installer.is_file():
        show_message("setup 폴더와 MigSetup.exe를 같은 위치에 두고 다시 실행하십시오.", "이관 도구 설치", 16)
        return 1
    result = run_installer(installer, root)
    if result:
        wheel_folder = select_wheel_folder()
        if wheel_folder is None:
            show_message("인터넷 설치에 실패했고 whl 폴더를 선택하지 않아 설치를 종료했습니다.", "이관 도구 설치", 16)
            return result
        result = run_installer(installer, root, wheel_folder)
        if result:
            show_message("whl 폴더 설치에 실패했습니다. 열린 창의 오류를 확인하십시오.", "이관 도구 설치", 16)
            return result
    show_message(
        "설치가 완료되었습니다.\napp\\.streamlit\\secrets.toml에 접속정보를 입력한 뒤 setup\\run.ps1을 실행하십시오.",
        "이관 도구 설치",
        64,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
