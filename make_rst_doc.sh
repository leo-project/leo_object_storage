#!/bin/sh

make doc
rm -rf doc/rst && mkdir doc/rst
pandoc --read=html --write=rst doc/leo_object_storage_api.html -o doc/rst/leo_object_storage_api.rst
pandoc --read=html --write=rst doc/leo_object_storage_haystack.html -o doc/rst/leo_object_storage_haystack.rst
pandoc --read=html --write=rst doc/leo_object_storage_server.html -o doc/rst/leo_object_storage_server.rst
pandoc --read=html --write=rst doc/leo_object_storage_transformer.html -o doc/rst/leo_object_storage_transformer.rst
pandoc --read=html --write=rst doc/leo_compact_fsm_controller.html -o doc/rst/leo_compact_fsm_controller.rst
pandoc --read=html --write=rst doc/leo_compact_fsm_worker.html -o doc/rst/leo_compact_fsm_worker.rst

for filename in doc/rst/leo_object_storage_api.rst \
                doc/rst/leo_object_storage_haystack.rst \
                doc/rst/leo_object_storage_server.rst \
                doc/rst/leo_object_storage_transformer.rst \
                doc/rst/leo_compact_fsm_controller.rst \
                doc/rst/leo_compact_fsm_worker.rst
do
    sed -ie "1,6d" "$filename"
    sed -ie "s/\Module //" "$filename"
    LINE_1=`cat $filename | wc -l`
    LINE_2=`expr $LINE_1 - 10`
    sed -ie "$LINE_2,\$d" "$filename"
done
rm doc/rst/*.rste
