#!/bin/sh

make doc
rm -rf doc/rst && mkdir doc/rst

for Mod in leo_compact_fsm_controller \
           leo_compact_fsm_worker \
           leo_object_storage_api \
           leo_object_storage_haystack \
           leo_object_storage_server \
           leo_object_storage_transformer
do
    read_file="doc/$Mod.html"
    write_file="doc/rst/$Mod.rst"
    pandoc --read=html --write=rst "$read_file" -o "$write_file"

    sed -ie "1,6d" "$write_file"
    sed -ie "1s/\Module //" "$write_file"
    LINE_1=`cat $write_file | wc -l`
    LINE_2=`expr $LINE_1 - 10`
    sed -ie "$LINE_2,\$d" "$write_file"
done
rm -rf doc/rst/*.rste
