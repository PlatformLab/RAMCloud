function portslay {
    pid=$(lsof -t -iTCP:$port)
    running=$?
    if [ $running -eq 0 ]; then
        kill $pid
    fi
}

OBJSUFFIX=$(git symbolic-ref -q HEAD | sed -e s,refs/heads/,.,)
OBJDIR=obj$OBJSUFFIX
export LD_LIBRARY_PATH=$OBJDIR
export PYTHONPATH=bindings/python

