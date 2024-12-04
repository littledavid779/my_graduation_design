#!/bin/bash

USER=$(whoami)
LOG_DIR="/home/hjl/Academical/BBTree/test/result/zoneio/"
SPDK_PATH="/home/chenbo/znstore/spdk/build/fio/spdk_nvme"
FIO_PATH="/home/chenbo/fio/fio"

# MAX_OPEN_ZONES=(1)
# MAX_CONCURRRENT_IO=(1 2 4 8 16 32 64)
# BS_SIZES=(4k 8k 16k 32k 64k 128k)
# # append/write test for SIGNLE zone under different IO_DEPTH and BS_SIZES
# for append in 0 1; do
#     for max_open_zones in ${MAX_OPEN_ZONES[@]}; do
#         for bs in ${BS_SIZES[@]}; do
#             LOG_NAME=${LOG_DIR}"fio_test_${append}_${max_open_zones}z_${bs}.log"
#             for iodepth in ${MAX_CONCURRRENT_IO[@]}; do
#                 echo "----------------test start------------------" | tee -a ${LOG_NAME}
#                 echo "append=$append max_open_zones=$max_open_zones concurrency=$iodepth" | tee -a ${LOG_NAME}

#                 if [ $append -eq 1 ]; then
#                     echo "zone append test" | tee -a ${LOG_NAME}
#                     sudo LD_PRELOAD=${SPDK_PATH} ${FIO_PATH} -name=Appendtest -group_reporting -zonemode=zbd \
#                         -ioengine=spdk -rw=write -zonesize=1037M -size=10z -direct=1 -numjobs=1 -thread=1 -time_based -runtime=1m '--filename=trtype=PCIe traddr=0000.c8.00.0 ns=1' \
#                         -max_open_zones=${max_open_zones} -bs=${bs} --job_max_open_zones=1 \
#                         -iodepth=$iodepth -zone_append=${append} | tee -a ${LOG_NAME}
#                 elif [ $append -eq 0 ]; then
#                     echo "zone write test" | tee -a ${LOG_NAME}
#                     sudo LD_PRELOAD=${SPDK_PATH} ${FIO_PATH} -name=Appendtest -group_reporting -zonemode=zbd \
#                         -ioengine=spdk -rw=write -zonesize=1037M -size=10z -direct=1 -iodepth=1 -thread=1 -time_based -runtime=1m '--filename=trtype=PCIe traddr=0000.c8.00.0 ns=1' \
#                         -max_open_zones=${max_open_zones} -bs=${bs} --job_max_open_zones=1 \
#                         -numjobs=${iodepth} -zone_append=${append} | tee -a ${LOG_NAME}
#                 fi
#                 echo "----------------test end--------------------" | tee -a ${LOG_NAME}
#                 sudo chown ${USER}:${USER} ${LOG_NAME}
#             done
#         done
#     done
# done

MAX_OPEN_ZONES=(1 4 8 12 14)
MAX_CONCURRRENT_IO=(1 2 4 8 16 32 64)
BS_SIZES=(4k 8k 16k 64k 128k)
# append/write test for MULTY zone under different IO_DEPTH and BS_SIZES
for append in 0 1; do
    for max_open_zones in ${MAX_OPEN_ZONES[@]}; do
        for bs in ${BS_SIZES[@]}; do
            LOG_NAME=${LOG_DIR}"fio_test_multyzone_${append}_${max_open_zones}z_${bs}.log"
            for iodepth in ${MAX_CONCURRRENT_IO[@]}; do
                echo "----------------test start------------------" | tee -a ${LOG_NAME}
                echo "append=$append max_open_zones=$max_open_zones iodepth=$iodepth" | tee -a ${LOG_NAME}

                if [ $append -eq 1 ]; then
                    echo "zone append test" | tee -a ${LOG_NAME}
                    sudo LD_PRELOAD=${SPDK_PATH} ${FIO_PATH} -name=Appendtest -group_reporting -zonemode=zbd \
                        -ioengine=spdk -rw=write -zonesize=1037M -size=4z -direct=1 -numjobs=1 -thread=1 -time_based -runtime=1m '--filename=trtype=PCIe traddr=0000.c8.00.0 ns=1' \
                        -max_open_zones=${max_open_zones} -bs=${bs} --job_max_open_zones=${max_open_zones} \
                        -iodepth=${iodepth} -zone_append=${append} | tee -a ${LOG_NAME}
                elif [ $append -eq 0 ]; then
                    echo "zone write test" | tee -a ${LOG_NAME}
                    sudo LD_PRELOAD=${SPDK_PATH} ${FIO_PATH} -name=Appendtest -group_reporting -zonemode=zbd \
                        -ioengine=spdk -rw=write -zonesize=1037M -size=4z -direct=1 -iodepth=1 -thread=1 -time_based -runtime=1m '--filename=trtype=PCIe traddr=0000.c8.00.0 ns=1' \
                        -max_open_zones=${max_open_zones} -bs=${bs} --job_max_open_zones=1 \
                        -numjobs=${iodepth} -zone_append=${append} | tee -a ${LOG_NAME}
                fi
                echo "----------------test end--------------------" | tee -a ${LOG_NAME}
                sudo chown ${USER}:${USER} ${LOG_NAME}
            done
        done
    done
done

# sudo fio --ioengine=psync --direct=1 --rw=write --group_reporting --zonemode=zbd --name=write --offset_increment=4z --size=4z --numjobs=8 --job_max_open_zones=1 --filename=/dev/nvme0n2 --time_based --runtime=2m --bs=4K --thread=1
# sudo fio --ioengine=libaio --direct=1 --rw=write --group_reporting --zonemode=zbd --name=write --offset_increment=4z --size=4z --numjobs=1 --job_max_open_zones=1 --filename=/dev/nvme0n2 --time_based --runtime=2m --bs=4K --iodepth=8

# exit 0

# echo "----------------test start------------------" | tee -a "fio_test.log"
# sudo LD_PRELOAD=/home/chenbo/znstore/spdk/build/fio/spdk_nvme /home/chenbo/fio/fio -name=Appendtest -ioengine=psync -group_reporting -rw=write --offset_increment=4z -zonemode=zbd -zonesize=2048M -max_open_zones=8 -bs=4K -size=10z -direct=1 -numjobs=8 -time_based -runtime=10 '--filename=trtype=PCIe traddr=0000.c8.00.0 ns=1' -iodepth=1 -thread=1 | tee -a "fio_test.log"
# echo "----------------test end--------------------" | tee -a "fio_test.log"
# echo "----------------test start------------------" | tee -a "fio_test.log"
# sudo LD_PRELOAD=/home/chenbo/znstore/spdk/build/fio/spdk_nvme /home/chenbo/fio/fio -name=Appendtest -ioengine=spdk -group_reporting -rw=write -zonemode=zbd -zonesize=2048M -max_open_zones=1 --job_max_open_zones=1 -bs=4K -size=10z -direct=1 -numjobs=1 -time_based -runtime=2m '--filename=trtype=PCIe traddr=0000.c8.00.0 ns=1' -iodepth=4 -thread=1 -zone_append=1 | tee -a "fio_test.log"
# echo "----------------test end--------------------" | tee -a "fio_test.log"
