import ctypes
import shutil
import subprocess
import sys
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def payload_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS) / "payload"
    return project_root()


def show_message(text: str, title: str, level: int) -> None:
    ctypes.windll.user32.MessageBoxW(None, text, title, level)


def select_folder(title: str) -> Path | None:
    command = [
        "powershell.exe",
        "-NoProfile",
        "-STA",
        "-Command",
        f"Add-Type -AssemblyName System.Windows.Forms; $dialog = New-Object System.Windows.Forms.FolderBrowserDialog; $dialog.Description = '{title}'; if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {{ [Console]::Write($dialog.SelectedPath) }}",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    selected = result.stdout.strip()
    return Path(selected) if selected else None


def copy_solution(source: Path, target: Path) -> None:
    if target.exists() and any(target.iterdir()):
        raise FileExistsError("선택한 설치 폴더가 비어 있지 않습니다.")
    shutil.copytree(source, target, dirs_exist_ok=True)
    (target / "artifact").mkdir(exist_ok=True)
    (target / "log").mkdir(exist_ok=True)
    if getattr(sys, "frozen", False):
        shutil.copy2(Path(sys.executable), target / "setup" / "MigSetup.exe")


def run_installer(installer: Path, root: Path, wheel_folder: Path | None = None) -> int:
    command = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(installer)]
    if wheel_folder:
        command.extend(["-WheelFolder", str(wheel_folder)])
    return subprocess.run(command, cwd=root, check=False).returncode


def main() -> int:
    source = payload_root()
    target = select_folder("솔루션 설치 폴더 선택")
    if target is None:
        return 0
    try:
        copy_solution(source, target)
    except OSError as error:
        show_message(f"솔루션 파일을 복사하지 못했습니다.\n{error}", "이관 도구 설치", 16)
        return 1
    installer = target / "setup" / "install.ps1"
    result = run_installer(installer, target)
    if result:
        wheel_folder = select_folder("서버에 복사한 whl 폴더 선택")
        if wheel_folder is None:
            show_message("인터넷 설치에 실패했고 whl 폴더를 선택하지 않아 설치를 종료했습니다.", "이관 도구 설치", 16)
            return result
        result = run_installer(installer, target, wheel_folder)
        if result:
            show_message("whl 폴더 설치에 실패했습니다. 열린 창의 오류를 확인하십시오.", "이관 도구 설치", 16)
            return result
    show_message(
        f"설치가 완료되었습니다.\n{target}\\app\\.streamlit\\secrets.toml에 접속정보를 입력한 뒤 {target}\\setup\\run.ps1을 실행하십시오.",
        "이관 도구 설치",
        64,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
