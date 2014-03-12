#!/bin/sh
let max_stripe_size=15
let istripe_size=2
let max_parity_size=10
let iparity_size=1
rm  -rf logs
mkdir logs
while [ $istripe_size -le $max_stripe_size ]
do
    sed "75 s/0/$istripe_size/" 1.xml > tmp.xml
    while [ $iparity_size -lt $istripe_size ] && [ $iparity_size -lt $max_parity_size ]
    do
        sed "76 s/0/$iparity_size/" tmp.xml > hdfs-site.xml
        
        for i in 1 2 3 4 5 6 7; do echo "stripe size$istripe_size" >tlog; echo "parity size $iparity_size">>tlog; ../bin/hadoop org.apache.hadoop.raid.RaidShell -rs_benchmark -verify -native >>tlog; mv tlog logs/test_$istripe_size.$iparity_size.$i.log; done
        
        let iparity_size=$iparity_size+1
    done

    let istripe_size=$istripe_size+1
done

