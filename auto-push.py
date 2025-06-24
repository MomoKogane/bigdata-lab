import subprocess
import os
import sys
from pathlib import Path

def execute_git_command(command, directory="."):
    """执行Git命令并返回结果"""
    try:
        result = subprocess.run(
            command,
            cwd=directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',  # 明确指定UTF-8编码
            errors='replace',  # 替换无法解码的字符
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"执行Git命令时出错: {e.stderr}")
        sys.exit(1)

def main():
    """主函数：执行git add、commit和push操作"""
    current_dir = os.getcwd()
    print(f"当前工作目录: {current_dir}")
    
    # 检查是否为Git仓库
    try:
        subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=True
        )
    except subprocess.CalledProcessError:
        print("错误：当前目录不是Git仓库。请先初始化Git仓库或克隆一个现有仓库。")
        sys.exit(1)
    
    # 获取当前分支名
    branch_output = execute_git_command(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    current_branch = branch_output.strip()
    print(f"当前分支: {current_branch}")
    
    # 执行git add .
    print("正在执行 'git add .'...")
    execute_git_command(["git", "add", "."])
    
    # 检查是否有文件需要提交
    status_output = execute_git_command(["git", "status", "--porcelain"])
    if not status_output:
        print("没有文件需要提交。")
        sys.exit(0)
    
    # 执行git commit
    commit_message = "这是一次修改"
    print(f"正在执行 'git commit -m \"{commit_message}\"'...")
    execute_git_command(["git", "commit", "-m", commit_message])
    
    # 执行git push
    remote_name = "origin"
    target_branch = "main"
    
    print(f"正在执行 'git push {remote_name} {current_branch}:{target_branch}'...")
    execute_git_command(["git", "push", remote_name, f"{current_branch}:{target_branch}"])
    
    print("所有文件已成功上传到GitHub仓库!")

if __name__ == "__main__":
    main()    