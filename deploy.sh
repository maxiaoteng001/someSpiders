echo '开始执行命令'
git pull

echo "创建虚拟环境"
python3 -m venv ./venv_somespiders

echo "生效虚拟环境"
source ./venv_somespiders/bin/activate

echo "安装依赖"
pip install -r requirement.txt

echo "杀掉历史进程"
ps axu | grep 'run_beike.py'|grep -v 'grep'|awk '{print $2}'|xargs kill -9

echo "启动 run_beike"
cd src/launcher && python3 run_beike.py >nohup.beike 2>&1 &
