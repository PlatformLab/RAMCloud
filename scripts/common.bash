function portslay {
    pid=$(lsof -t -i:$1)
    running=$?
    if [ $running -eq 0 ]; then
        kill $pid
    fi
}

# Register a shell command to be executed just before the shell exits.
function atexit {
    task=$1
    tasks=$(trap -p EXIT)
    tasks=${tasks##trap -- \'}
    tasks=${tasks%%\' EXIT}
    trap "$tasks$task;" EXIT
}

OBJSUFFIX=$(git symbolic-ref -q HEAD | sed -e s,refs/heads/,.,)
OBJDIR=obj$OBJSUFFIX
APPOBJDIR=$OBJDIR/apps
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OBJDIR:/lib:/usr/lib:/usr/local/lib
export PYTHONPATH=bindings/python

if [ ! -x $OBJDIR/coordinator ]; then
    echo "Error: $OBJDIR/coordinator doesn't exist" > /dev/stderr
    exit 1
fi
if [ ! -x $OBJDIR/server ]; then
    echo "Error: $OBJDIR/server doesn't exist" > /dev/stderr
    exit 1
fi
