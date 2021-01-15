data="""
476:483 trimmed to 483:483
706:714 trimmed to 714:714
1909:1919 trimmed to 1919:1919
65:71 trimmed to 71:71
1221:1229 trimmed to 1229:1229
1115:1125 trimmed to 1125:1125
1361:1404 trimmed to 1368:1404
1297:1303 trimmed to 1303:1303
1099:1105 trimmed to 1105:1105
169:174 trimmed to 174:174
965:969 trimmed to 969:969
983:990 trimmed to 990:990
26:31 trimmed to 31:31
95:108 trimmed to 108:108
670:675 trimmed to 675:675
81:87 trimmed to 87:87
81:87 trimmed to 87:87
81:87 trimmed to 87:87
830:834 trimmed to 834:834
1544:1559 trimmed to 1552:1559
1553:1559 trimmed to 1559:1559
329:335 trimmed to 335:335
1531:1534 trimmed to 1534:1534
1531:1538 trimmed to 1534:1538
1531:1538 trimmed to 1538:1538
1535:1538 trimmed to 1538:1538
1189:1196 trimmed to 1196:1196
1270:1279 trimmed to 1279:1279
1261:1285 trimmed to 1270:1285
1749:1758 trimmed to 1758:1758
"""

lines = data.split("\n")
for line in lines:
    if line != '':
        s1, middle, e2 = line.split(":")
        s2, trimmed, to, e1 = middle.split(" ")
        orig_len = int(s2) - int(s1)
        new_len = int(e2) - int(e1)
        print("len {} -> {}".format(orig_len, new_len))

data = """
< 0,0,1024089,1024092
< 0,0,1024089,1024096
< 0,0,1024089,1024096
< 0,0,1024093,1024096
> 0,0,1024092,1024092
> 0,0,1024092,1024096
> 0,0,1024096,1024096
> 0,0,1024096,1024096
< 0,0,1436454,1436463
> 0,0,1436463,1436463
< 0,0,468701,468705
> 0,0,468705,468705
< 0,0,76899,76905
< 0,0,76899,76905
< 0,0,76899,76905
> 0,0,76905,76905
> 0,0,76905,76905
> 0,0,76905,76905
< 0,0,778595,778610
< 0,0,778604,778610
> 0,0,778603,778610
> 0,0,778610,778610
< 1,0,1055660,1055667
> 1,0,1055667,1055667
< 1,0,1069961,1069970
> 1,0,1069970,1069970
< 1,0,1295641,1295665
> 1,0,1295650,1295665
< 1,0,861169,861175
> 1,0,861175,861175
"""

lines = data.split("\n")
for line in lines:
    if line != '':
        if line[0] == "<":
            status="orig"
        else:
            status="new"
        line = line[2:]
        user,topic,start,end = line.split(",")
        length = int(end) - int(start)
        if status == "orig":
            print("{} {}".format(status, length))
