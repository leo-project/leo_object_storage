max=10
if [ $# -ge 1 ]; then
    max=$1
fi
for ((i=0; i < $max; i++)); do
    make eunit
    if [ $? -ne 0 ]; then
        exit $?
    fi
done
echo "[test] ${max} times tests Done!"
