import os

import pandas as pd
import numpy as np
import re

dd_info_origin = {'AAVEUSDT': 0.8505333471416735, 'BNBUSDT': 0.5948814572860666, 'BTCUSDT': 0.5318857287742109, 'DOGEUSDT': 0.8211751152073733, 'ETHUSDT': 0.6967123668981599, 'KASUSDT': 0.9422474924934516, 'LINKUSDT': 0.7492373398413666, 'PENDLEUSDT': 0.8459187085064718, 'RENDERUSDT': 0.9437119113573408, 'RUNEUSDT': 0.9434807692307692, 'SKYUSDT': 0.577160867983998, 'SOLUSDT': 0.7969215155615696, 'STXUSDT': 0.9117176671286916, 'TONUSDT': 0.906029582962405, 'TRXUSDT': 0.280603704303688, 'UNIUSDT': 0.8557188498402556}
margin_info = {
    1.1: {
        'target_loss_percent': 1.1,
        'actual_margin_needed': 18.55617,
        'margin_ratio': 1.0
    },
    1.2: {
        'target_loss_percent': 1.2,
        'actual_margin_needed': 20.750814,
        'margin_ratio': 1.1182703111687378
    },
    1.3: {
        'target_loss_percent': 1.3,
        'actual_margin_needed': 23.045663,
        'margin_ratio': 1.2419407129811808
    },
    1.4: {
        'target_loss_percent': 1.4,
        'actual_margin_needed': 25.440817,
        'margin_ratio': 1.371016594480434
    },
    1.5: {
        'target_loss_percent': 1.5,
        'actual_margin_needed': 27.936377,
        'margin_ratio': 1.5055033986000343
    },
    1.6: {
        'target_loss_percent': 1.6,
        'actual_margin_needed': 30.53244,
        'margin_ratio': 1.6454063527117933
    },
    1.7000000000000002: {
        'target_loss_percent': 1.7000000000000002,
        'actual_margin_needed': 33.229108,
        'margin_ratio': 1.7907309536396785
    },
    1.8: {
        'target_loss_percent': 1.8,
        'actual_margin_needed': 36.026479,
        'margin_ratio': 1.9414824826459338
    },
    1.9: {
        'target_loss_percent': 1.9,
        'actual_margin_needed': 38.924652,
        'margin_ratio': 2.097666274883233
    },
    2.0: {
        'target_loss_percent': 2.0,
        'actual_margin_needed': 41.923728,
        'margin_ratio': 2.259287773285112
    },
    2.1: {
        'target_loss_percent': 2.1,
        'actual_margin_needed': 45.023804,
        'margin_ratio': 2.4263522052233837
    },
    2.2: {
        'target_loss_percent': 2.2,
        'actual_margin_needed': 48.22498,
        'margin_ratio': 2.5988649597411535
    },
    2.3: {
        'target_loss_percent': 2.3,
        'actual_margin_needed': 51.527355,
        'margin_ratio': 2.776831371991095
    },
    2.4000000000000004: {
        'target_loss_percent': 2.4000000000000004,
        'actual_margin_needed': 54.931028,
        'margin_ratio': 2.960256777125883
    },
    2.5: {
        'target_loss_percent': 2.5,
        'actual_margin_needed': 58.436097,
        'margin_ratio': 3.1491464564077605
    },
    2.6: {
        'target_loss_percent': 2.6,
        'actual_margin_needed': 62.042661,
        'margin_ratio': 3.343505744989402
    },
    2.7: {
        'target_loss_percent': 2.7,
        'actual_margin_needed': 65.750818,
        'margin_ratio': 3.5433399241330505
    },
    2.8: {
        'target_loss_percent': 2.8,
        'actual_margin_needed': 69.560667,
        'margin_ratio': 3.7486543289913805
    },
    2.9000000000000004: {
        'target_loss_percent': 2.9000000000000004,
        'actual_margin_needed': 73.472306,
        'margin_ratio': 3.959454240826636
    },
    3.0: {
        'target_loss_percent': 3.0,
        'actual_margin_needed': 77.485834,
        'margin_ratio': 4.175744994791489
    },
    3.1: {
        'target_loss_percent': 3.1,
        'actual_margin_needed': 81.601348,
        'margin_ratio': 4.397531818257755
    },
    3.2: {
        'target_loss_percent': 3.2,
        'actual_margin_needed': 85.818947,
        'margin_ratio': 4.624820046378105
    },
    3.3000000000000003: {
        'target_loss_percent': 3.3000000000000003,
        'actual_margin_needed': 90.138728,
        'margin_ratio': 4.857614906524352
    },
    3.4000000000000004: {
        'target_loss_percent': 3.4000000000000004,
        'actual_margin_needed': 94.560789,
        'margin_ratio': 5.0959216799587415
    },
    3.5: {
        'target_loss_percent': 3.5,
        'actual_margin_needed': 99.085228,
        'margin_ratio': 5.339745647943514
    },
    3.6: {
        'target_loss_percent': 3.6,
        'actual_margin_needed': 103.712143,
        'margin_ratio': 5.5890920917409135
    },
    3.7: {
        'target_loss_percent': 3.7,
        'actual_margin_needed': 108.441631,
        'margin_ratio': 5.843966238722754
    },
    3.8000000000000003: {
        'target_loss_percent': 3.8000000000000003,
        'actual_margin_needed': 113.273789,
        'margin_ratio': 6.104373316260844
    },
    3.9000000000000004: {
        'target_loss_percent': 3.9000000000000004,
        'actual_margin_needed': 118.208716,
        'margin_ratio': 6.370318659507861
    },
    4.0: {
        'target_loss_percent': 4.0,
        'actual_margin_needed': 123.246507,
        'margin_ratio': 6.641807388054754
    },
    4.1: {
        'target_loss_percent': 4.1,
        'actual_margin_needed': 128.38726,
        'margin_ratio': 6.918844783163767
    },
    4.2: {
        'target_loss_percent': 4.2,
        'actual_margin_needed': 133.631073,
        'margin_ratio': 7.201436126097141
    },
    4.300000000000001: {
        'target_loss_percent': 4.300000000000001,
        'actual_margin_needed': 138.978042,
        'margin_ratio': 7.489586590336259
    },
    4.4: {
        'target_loss_percent': 4.4,
        'actual_margin_needed': 144.428264,
        'margin_ratio': 7.783301403252934
    },
    4.5: {
        'target_loss_percent': 4.5,
        'actual_margin_needed': 150.938854,
        'margin_ratio': 8.134159904764829
    },
    4.6: {
        'target_loss_percent': 4.6,
        'actual_margin_needed': 156.599415,
        'margin_ratio': 8.439209977058843
    },
    4.7: {
        'target_loss_percent': 4.7,
        'actual_margin_needed': 162.363616,
        'margin_ratio': 8.74984525362723
    },
    4.800000000000001: {
        'target_loss_percent': 4.800000000000001,
        'actual_margin_needed': 168.231552,
        'margin_ratio': 9.06607085406094
    },
    4.9: {
        'target_loss_percent': 4.9,
        'actual_margin_needed': 174.203321,
        'margin_ratio': 9.387892059622216
    },
    5.0: {
        'target_loss_percent': 5.0,
        'actual_margin_needed': 180.279017,
        'margin_ratio': 9.71531393601158
    },
    5.1000000000000005: {
        'target_loss_percent': 5.1000000000000005,
        'actual_margin_needed': 186.458738,
        'margin_ratio': 10.048341764491271
    },
    5.2: {
        'target_loss_percent': 5.2,
        'actual_margin_needed': 192.742579,
        'margin_ratio': 10.386980664652242
    },
    5.3: {
        'target_loss_percent': 5.3,
        'actual_margin_needed': 199.130637,
        'margin_ratio': 10.731235863866305
    },
    5.4: {
        'target_loss_percent': 5.4,
        'actual_margin_needed': 205.623006,
        'margin_ratio': 11.081112427833975
    },
    5.5: {
        'target_loss_percent': 5.5,
        'actual_margin_needed': 212.219783,
        'margin_ratio': 11.43661558392707
    },
    5.6000000000000005: {
        'target_loss_percent': 5.6000000000000005,
        'actual_margin_needed': 218.921063,
        'margin_ratio': 11.797750451736537
    },
    5.7: {
        'target_loss_percent': 5.7,
        'actual_margin_needed': 225.726942,
        'margin_ratio': 12.164522204743758
    },
    5.800000000000001: {
        'target_loss_percent': 5.800000000000001,
        'actual_margin_needed': 232.637515,
        'margin_ratio': 12.536935962539683
    },
    5.9: {
        'target_loss_percent': 5.9,
        'actual_margin_needed': 239.652878,
        'margin_ratio': 12.914996898605692
    },
    6.0: {
        'target_loss_percent': 6.0,
        'actual_margin_needed': 246.773125,
        'margin_ratio': 13.298710078642305
    },
    6.1000000000000005: {
        'target_loss_percent': 6.1000000000000005,
        'actual_margin_needed': 253.998352,
        'margin_ratio': 13.688080676130904
    },
    6.2: {
        'target_loss_percent': 6.2,
        'actual_margin_needed': 261.328654,
        'margin_ratio': 14.083113810662434
    },
    6.300000000000001: {
        'target_loss_percent': 6.300000000000001,
        'actual_margin_needed': 269.704861,
        'margin_ratio': 14.534511216484866
    },
    6.4: {
        'target_loss_percent': 6.4,
        'actual_margin_needed': 277.250956,
        'margin_ratio': 14.941173528804702
    },
    6.5: {
        'target_loss_percent': 6.5,
        'actual_margin_needed': 284.902505,
        'margin_ratio': 15.353518802640846
    },
    6.6000000000000005: {
        'target_loss_percent': 6.6000000000000005,
        'actual_margin_needed': 292.659602,
        'margin_ratio': 15.77155210369381
    },
    6.7: {
        'target_loss_percent': 6.7,
        'actual_margin_needed': 300.522343,
        'margin_ratio': 16.19527860544498
    },
    6.800000000000001: {
        'target_loss_percent': 6.800000000000001,
        'actual_margin_needed': 308.490821,
        'margin_ratio': 16.62470331970444
    },
    6.9: {
        'target_loss_percent': 6.9,
        'actual_margin_needed': 316.56513,
        'margin_ratio': 17.059831312172715
    },
    7.0: {
        'target_loss_percent': 7.0,
        'actual_margin_needed': 324.745365,
        'margin_ratio': 17.50066770244075
    },
    7.1000000000000005: {
        'target_loss_percent': 7.1000000000000005,
        'actual_margin_needed': 333.031619,
        'margin_ratio': 17.947217502318633
    },
    7.2: {
        'target_loss_percent': 7.2,
        'actual_margin_needed': 341.423988,
        'margin_ratio': 18.39948588528775
    },
    7.300000000000001: {
        'target_loss_percent': 7.300000000000001,
        'actual_margin_needed': 349.922564,
        'margin_ratio': 18.857477809267753
    },
    7.4: {
        'target_loss_percent': 7.4,
        'actual_margin_needed': 358.527441,
        'margin_ratio': 19.32119833995916
    },
    7.5: {
        'target_loss_percent': 7.5,
        'actual_margin_needed': 367.238714,
        'margin_ratio': 19.790652596952928
    },
    7.6000000000000005: {
        'target_loss_percent': 7.6000000000000005,
        'actual_margin_needed': 376.980818,
        'margin_ratio': 20.31565878087989
    },
    7.7: {
        'target_loss_percent': 7.7,
        'actual_margin_needed': 385.911838,
        'margin_ratio': 20.796955298426344
    },
    7.800000000000001: {
        'target_loss_percent': 7.800000000000001,
        'actual_margin_needed': 394.949626,
        'margin_ratio': 21.28400558951551
    },
    7.9: {
        'target_loss_percent': 7.9,
        'actual_margin_needed': 404.094276,
        'margin_ratio': 21.776814719847895
    },
    8.0: {
        'target_loss_percent': 8.0,
        'actual_margin_needed': 413.345882,
        'margin_ratio': 22.275387755124036
    },
    8.100000000000001: {
        'target_loss_percent': 8.100000000000001,
        'actual_margin_needed': 422.704536,
        'margin_ratio': 22.779729653263576
    },
    8.2: {
        'target_loss_percent': 8.2,
        'actual_margin_needed': 432.170331,
        'margin_ratio': 23.289845426076607
    },
    8.3: {
        'target_loss_percent': 8.3,
        'actual_margin_needed': 441.743361,
        'margin_ratio': 23.805740139263648
    },
    8.4: {
        'target_loss_percent': 8.4,
        'actual_margin_needed': 451.423718,
        'margin_ratio': 24.32741875074436
    },
    8.5: {
        'target_loss_percent': 8.5,
        'actual_margin_needed': 461.211494,
        'margin_ratio': 24.854886218438395
    },
    8.600000000000001: {
        'target_loss_percent': 8.600000000000001,
        'actual_margin_needed': 471.106783,
        'margin_ratio': 25.38814760804627
    },
    8.7: {
        'target_loss_percent': 8.7,
        'actual_margin_needed': 481.109676,
        'margin_ratio': 25.927207823597215
    },
    8.8: {
        'target_loss_percent': 8.8,
        'actual_margin_needed': 492.138646,
        'margin_ratio': 26.521563770972133
    },
    8.9: {
        'target_loss_percent': 8.9,
        'actual_margin_needed': 502.364907,
        'margin_ratio': 27.072661384326615
    },
    9.0: {
        'target_loss_percent': 9.0,
        'actual_margin_needed': 512.699142,
        'margin_ratio': 27.629577763083653
    },
    9.1: {
        'target_loss_percent': 9.1,
        'actual_margin_needed': 523.141443,
        'margin_ratio': 28.192317865162906
    },
    9.200000000000001: {
        'target_loss_percent': 9.200000000000001,
        'actual_margin_needed': 533.691902,
        'margin_ratio': 28.760886648484036
    },
    9.3: {
        'target_loss_percent': 9.3,
        'actual_margin_needed': 544.35061,
        'margin_ratio': 29.335289017076256
    },
    9.4: {
        'target_loss_percent': 9.4,
        'actual_margin_needed': 555.117659,
        'margin_ratio': 29.91552992885924
    },
    9.5: {
        'target_loss_percent': 9.5,
        'actual_margin_needed': 565.993142,
        'margin_ratio': 30.501614395643067
    },
    9.6: {
        'target_loss_percent': 9.6,
        'actual_margin_needed': 576.977148,
        'margin_ratio': 31.0935472136761
    },
    9.700000000000001: {
        'target_loss_percent': 9.700000000000001,
        'actual_margin_needed': 588.069771,
        'margin_ratio': 31.69133344865885
    },
    9.8: {
        'target_loss_percent': 9.8,
        'actual_margin_needed': 600.18123,
        'margin_ratio': 32.344025194854325
    },
    9.9: {
        'target_loss_percent': 9.9,
        'actual_margin_needed': 611.500249,
        'margin_ratio': 32.95401200786585
    },
    10.0: {
        'target_loss_percent': 10.0,
        'actual_margin_needed': 622.928249,
        'margin_ratio': 33.569871853944
    },
    10.1: {
        'target_loss_percent': 10.1,
        'actual_margin_needed': 634.465321,
        'margin_ratio': 34.191609637118
    },
    10.200000000000001: {
        'target_loss_percent': 10.200000000000001,
        'actual_margin_needed': 646.111555,
        'margin_ratio': 34.81923020752666
    },
    10.3: {
        'target_loss_percent': 10.3,
        'actual_margin_needed': 657.867044,
        'margin_ratio': 35.45273857698005
    },
    10.4: {
        'target_loss_percent': 10.4,
        'actual_margin_needed': 669.731877,
        'margin_ratio': 36.09213954172655
    },
    10.5: {
        'target_loss_percent': 10.5,
        'actual_margin_needed': 681.706145,
        'margin_ratio': 36.737438005795376
    },
    10.600000000000001: {
        'target_loss_percent': 10.600000000000001,
        'actual_margin_needed': 693.789939,
        'margin_ratio': 37.388638873215754
    },
    10.700000000000001: {
        'target_loss_percent': 10.700000000000001,
        'actual_margin_needed': 706.886465,
        'margin_ratio': 38.09441630465769
    },
    10.8: {
        'target_loss_percent': 10.8,
        'actual_margin_needed': 719.199379,
        'margin_ratio': 38.75796454764102
    },
    10.9: {
        'target_loss_percent': 10.9,
        'actual_margin_needed': 731.62218,
        'margin_ratio': 39.42743464842152
    },
    11.0: {
        'target_loss_percent': 11.0,
        'actual_margin_needed': 744.154957,
        'margin_ratio': 40.10283140324754
    },
    11.100000000000001: {
        'target_loss_percent': 11.100000000000001,
        'actual_margin_needed': 756.797802,
        'margin_ratio': 40.78415977003875
    },
    11.200000000000001: {
        'target_loss_percent': 11.200000000000001,
        'actual_margin_needed': 769.550805,
        'margin_ratio': 41.471424598933936
    },
    11.3: {
        'target_loss_percent': 11.3,
        'actual_margin_needed': 782.414054,
        'margin_ratio': 42.164630632291036
    },
    11.4: {
        'target_loss_percent': 11.4,
        'actual_margin_needed': 795.38764,
        'margin_ratio': 42.863782774139274
    },
    11.5: {
        'target_loss_percent': 11.5,
        'actual_margin_needed': 809.36618,
        'margin_ratio': 43.61709232023634
    },
    11.600000000000001: {
        'target_loss_percent': 11.600000000000001,
        'actual_margin_needed': 822.571314,
        'margin_ratio': 44.328722683614124
    },
    11.700000000000001: {
        'target_loss_percent': 11.700000000000001,
        'actual_margin_needed': 835.887143,
        'margin_ratio': 45.04631844825737
    },
    11.8: {
        'target_loss_percent': 11.8,
        'actual_margin_needed': 849.313756,
        'margin_ratio': 45.76988441041443
    },
    11.9: {
        'target_loss_percent': 11.9,
        'actual_margin_needed': 862.851242,
        'margin_ratio': 46.49942536633367
    },
    12.0: {
        'target_loss_percent': 12.0,
        'actual_margin_needed': 876.499691,
        'margin_ratio': 47.2349461661539
    },
    12.100000000000001: {
        'target_loss_percent': 12.100000000000001,
        'actual_margin_needed': 890.259191,
        'margin_ratio': 47.97645155223302
    },
    12.200000000000001: {
        'target_loss_percent': 12.200000000000001,
        'actual_margin_needed': 905.011702,
        'margin_ratio': 48.77147072914292
    },
    12.3: {
        'target_loss_percent': 12.3,
        'actual_margin_needed': 919.00489,
        'margin_ratio': 49.52556966227406
    },
    12.4: {
        'target_loss_percent': 12.4,
        'actual_margin_needed': 933.109486,
        'margin_ratio': 50.28567242054798
    },
    12.5: {
        'target_loss_percent': 12.5,
        'actual_margin_needed': 947.325576,
        'margin_ratio': 51.051783638541785
    },
    12.600000000000001: {
        'target_loss_percent': 12.600000000000001,
        'actual_margin_needed': 961.65325,
        'margin_ratio': 51.82390816639425
    },
    12.700000000000001: {
        'target_loss_percent': 12.700000000000001,
        'actual_margin_needed': 976.092597,
        'margin_ratio': 52.60205080035373
    },
    12.8: {
        'target_loss_percent': 12.8,
        'actual_margin_needed': 990.643705,
        'margin_ratio': 53.38621628277817
    },
    12.9: {
        'target_loss_percent': 12.9,
        'actual_margin_needed': 1006.181554,
        'margin_ratio': 54.223557663030675
    },
    13.0: {
        'target_loss_percent': 13.0,
        'actual_margin_needed': 1020.968473,
        'margin_ratio': 55.02043110189225
    },
    13.100000000000001: {
        'target_loss_percent': 13.100000000000001,
        'actual_margin_needed': 1035.867504,
        'margin_ratio': 55.823346304760086
    },
    13.200000000000001: {
        'target_loss_percent': 13.200000000000001,
        'actual_margin_needed': 1050.878737,
        'margin_ratio': 56.63230812177297
    },
    13.3: {
        'target_loss_percent': 13.3,
        'actual_margin_needed': 1066.002258,
        'margin_ratio': 57.447321187507974
    },
    13.4: {
        'target_loss_percent': 13.4,
        'actual_margin_needed': 1081.238156,
        'margin_ratio': 58.26839029821347
    },
    13.5: {
        'target_loss_percent': 13.5,
        'actual_margin_needed': 1096.586518,
        'margin_ratio': 59.09552014235696
    },
    13.600000000000001: {
        'target_loss_percent': 13.600000000000001,
        'actual_margin_needed': 1112.920984,
        'margin_ratio': 59.97579155612392
    },
    13.700000000000001: {
        'target_loss_percent': 13.700000000000001,
        'actual_margin_needed': 1128.507263,
        'margin_ratio': 60.81574284995233
    },
    13.8: {
        'target_loss_percent': 13.8,
        'actual_margin_needed': 1144.206355,
        'margin_ratio': 61.66177368497917
    },
    13.9: {
        'target_loss_percent': 13.9,
        'actual_margin_needed': 1160.018349,
        'margin_ratio': 62.5138888574528
    },
    14.0: {
        'target_loss_percent': 14.0,
        'actual_margin_needed': 1175.943331,
        'margin_ratio': 63.37209300195028
    },
    14.100000000000001: {
        'target_loss_percent': 14.100000000000001,
        'actual_margin_needed': 1191.981387,
        'margin_ratio': 64.23639075304871
    },
    14.200000000000001: {
        'target_loss_percent': 14.200000000000001,
        'actual_margin_needed': 1208.997073,
        'margin_ratio': 65.15337340625786
    },
    14.3: {
        'target_loss_percent': 14.3,
        'actual_margin_needed': 1225.274876,
        'margin_ratio': 66.03059122653003
    },
    14.4: {
        'target_loss_percent': 14.4,
        'actual_margin_needed': 1241.666101,
        'margin_ratio': 66.91392140727315
    },
    14.5: {
        'target_loss_percent': 14.5,
        'actual_margin_needed': 1258.170835,
        'margin_ratio': 67.8033686369547
    },
    14.600000000000001: {
        'target_loss_percent': 14.600000000000001,
        'actual_margin_needed': 1274.789164,
        'margin_ratio': 68.69893755015178
    },
    14.700000000000001: {
        'target_loss_percent': 14.700000000000001,
        'actual_margin_needed': 1291.521175,
        'margin_ratio': 69.60063283533187
    },
    14.8: {
        'target_loss_percent': 14.8,
        'actual_margin_needed': 1309.226587,
        'margin_ratio': 70.55478511998975
    },
    14.9: {
        'target_loss_percent': 14.9,
        'actual_margin_needed': 1326.200161,
        'margin_ratio': 71.46949833936635
    },
    15.0: {
        'target_loss_percent': 15.0,
        'actual_margin_needed': 1343.28776,
        'margin_ratio': 72.39035641514384
    },
    15.100000000000001: {
        'target_loss_percent': 15.100000000000001,
        'actual_margin_needed': 1360.489473,
        'margin_ratio': 73.31736414357057
    },
    15.200000000000001: {
        'target_loss_percent': 15.200000000000001,
        'actual_margin_needed': 1377.805383,
        'margin_ratio': 74.25052599755229
    },
    15.3: {
        'target_loss_percent': 15.3,
        'actual_margin_needed': 1395.235578,
        'margin_ratio': 75.18984671944695
    },
    15.4: {
        'target_loss_percent': 15.4,
        'actual_margin_needed': 1413.639162,
        'margin_ratio': 76.18162379413424
    },
    15.5: {
        'target_loss_percent': 15.5,
        'actual_margin_needed': 1431.312723,
        'margin_ratio': 77.13405961467264
    },
    15.600000000000001: {
        'target_loss_percent': 15.600000000000001,
        'actual_margin_needed': 1449.10091,
        'margin_ratio': 78.09267267976097
    },
    15.700000000000001: {
        'target_loss_percent': 15.700000000000001,
        'actual_margin_needed': 1467.003809,
        'margin_ratio': 79.05746762397628
    },
    15.8: {
        'target_loss_percent': 15.8,
        'actual_margin_needed': 1485.021505,
        'margin_ratio': 80.02844902800523
    },
    15.9: {
        'target_loss_percent': 15.9,
        'actual_margin_needed': 1504.00163,
        'margin_ratio': 81.05129614570247
    },
    16.0: {
        'target_loss_percent': 16.0,
        'actual_margin_needed': 1522.264228,
        'margin_ratio': 82.03547542407726
    },
    16.1: {
        'target_loss_percent': 16.1,
        'actual_margin_needed': 1540.641964,
        'margin_ratio': 83.02585953890268
    },
    16.200000000000003: {
        'target_loss_percent': 16.200000000000003,
        'actual_margin_needed': 1559.134922,
        'margin_ratio': 84.02245301697494
    },
    16.3: {
        'target_loss_percent': 16.3,
        'actual_margin_needed': 1577.743187,
        'margin_ratio': 85.02526043898067
    },
    16.4: {
        'target_loss_percent': 16.4,
        'actual_margin_needed': 1597.305977,
        'margin_ratio': 86.07950762468764
    },
    16.5: {
        'target_loss_percent': 16.5,
        'actual_margin_needed': 1616.160671,
        'margin_ratio': 87.09559521172741
    },
    16.6: {
        'target_loss_percent': 16.6,
        'actual_margin_needed': 1635.13101,
        'margin_ratio': 88.11791495766637
    },
    16.700000000000003: {
        'target_loss_percent': 16.700000000000003,
        'actual_margin_needed': 1654.217079,
        'margin_ratio': 89.14647144319113
    },
    16.8: {
        'target_loss_percent': 16.8,
        'actual_margin_needed': 1673.418962,
        'margin_ratio': 90.1812691950979
    },
    16.9: {
        'target_loss_percent': 16.9,
        'actual_margin_needed': 1693.570507,
        'margin_ratio': 91.2672446415397
    },
    17.0: {
        'target_loss_percent': 17.0,
        'actual_margin_needed': 1713.020336,
        'margin_ratio': 92.31540431026446
    },
    17.1: {
        'target_loss_percent': 17.1,
        'actual_margin_needed': 1732.586316,
        'margin_ratio': 93.36982340644647
    },
    17.2: {
        'target_loss_percent': 17.2,
        'actual_margin_needed': 1752.26853,
        'margin_ratio': 94.43050640299155
    },
    17.3: {
        'target_loss_percent': 17.3,
        'actual_margin_needed': 1772.067061,
        'margin_ratio': 95.49745777280548
    },
    17.400000000000002: {
        'target_loss_percent': 17.400000000000002,
        'actual_margin_needed': 1792.813412,
        'margin_ratio': 96.61548757098042
    },
    17.5: {
        'target_loss_percent': 17.5,
        'actual_margin_needed': 1812.861399,
        'margin_ratio': 97.6958822321632
    },
    17.6: {
        'target_loss_percent': 17.6,
        'actual_margin_needed': 1833.026037,
        'margin_ratio': 98.7825632660188
    },
    17.7: {
        'target_loss_percent': 17.7,
        'actual_margin_needed': 1853.307411,
        'margin_ratio': 99.87553525323382
    },
    17.8: {
        'target_loss_percent': 17.8,
        'actual_margin_needed': 1873.705604,
        'margin_ratio': 100.97480266671408
    },
    17.900000000000002: {
        'target_loss_percent': 17.900000000000002,
        'actual_margin_needed': 1895.052777,
        'margin_ratio': 102.1252110214554
    },
    18.0: {
        'target_loss_percent': 18.0,
        'actual_margin_needed': 1915.701925,
        'margin_ratio': 103.23800250806066
    },
    18.1: {
        'target_loss_percent': 18.1,
        'actual_margin_needed': 1936.468223,
        'margin_ratio': 104.35710725866383
    },
    18.2: {
        'target_loss_percent': 18.2,
        'actual_margin_needed': 1957.351754,
        'margin_ratio': 105.48252974617067
    },
    18.3: {
        'target_loss_percent': 18.3,
        'actual_margin_needed': 1979.17085,
        'margin_ratio': 106.65837023480599
    },
    18.400000000000002: {
        'target_loss_percent': 18.400000000000002,
        'actual_margin_needed': 2000.306579,
        'margin_ratio': 107.79738378124364
    },
    18.5: {
        'target_loss_percent': 18.5,
        'actual_margin_needed': 2021.559873,
        'margin_ratio': 108.94273295620809
    },
    18.6: {
        'target_loss_percent': 18.6,
        'actual_margin_needed': 2042.930813,
        'margin_ratio': 110.09442212482423
    },
    18.7: {
        'target_loss_percent': 18.7,
        'actual_margin_needed': 2064.419482,
        'margin_ratio': 111.25245575999787
    },
    18.8: {
        'target_loss_percent': 18.8,
        'actual_margin_needed': 2086.850337,
        'margin_ratio': 112.46126420484397
    },
    18.900000000000002: {
        'target_loss_percent': 18.900000000000002,
        'actual_margin_needed': 2108.592686,
        'margin_ratio': 113.63296876456724
    },
    19.0: {
        'target_loss_percent': 19.0,
        'actual_margin_needed': 2130.453093,
        'margin_ratio': 114.81103552079982
    },
    19.1: {
        'target_loss_percent': 19.1,
        'actual_margin_needed': 2152.43164,
        'margin_ratio': 115.99546889255701
    },
    19.2: {
        'target_loss_percent': 19.2,
        'actual_margin_needed': 2175.34348,
        'margin_ratio': 117.23019782638335
    },
    19.3: {
        'target_loss_percent': 19.3,
        'actual_margin_needed': 2197.576937,
        'margin_ratio': 118.42836840792036
    },
    19.400000000000002: {
        'target_loss_percent': 19.400000000000002,
        'actual_margin_needed': 2219.92886,
        'margin_ratio': 119.63292317326257
    },
    19.5: {
        'target_loss_percent': 19.5,
        'actual_margin_needed': 2242.399331,
        'margin_ratio': 120.8438665414253
    },
    19.6: {
        'target_loss_percent': 19.6,
        'actual_margin_needed': 2265.796243,
        'margin_ratio': 122.10473621442354
    },
    19.7: {
        'target_loss_percent': 19.7,
        'actual_margin_needed': 2288.522847,
        'margin_ratio': 123.32948270036327
    },
    19.8: {
        'target_loss_percent': 19.8,
        'actual_margin_needed': 2311.368324,
        'margin_ratio': 124.56063530351359
    },
    19.900000000000002: {
        'target_loss_percent': 19.900000000000002,
        'actual_margin_needed': 2334.332756,
        'margin_ratio': 125.79819844288987
    },
    20.0: {
        'target_loss_percent': 20.0,
        'actual_margin_needed': 2358.218807,
        'margin_ratio': 127.08542802744316
    },
    20.1: {
        'target_loss_percent': 20.1,
        'actual_margin_needed': 2381.440588,
        'margin_ratio': 128.33685981536058
    },
    20.200000000000003: {
        'target_loss_percent': 20.200000000000003,
        'actual_margin_needed': 2404.781648,
        'margin_ratio': 129.59471960000366
    },
    20.3: {
        'target_loss_percent': 20.3,
        'actual_margin_needed': 2428.242066,
        'margin_ratio': 130.85901163871637
    },
    20.400000000000002: {
        'target_loss_percent': 20.400000000000002,
        'actual_margin_needed': 2452.621302,
        'margin_ratio': 132.17281917550872
    },
    20.5: {
        'target_loss_percent': 20.5,
        'actual_margin_needed': 2476.340281,
        'margin_ratio': 133.45104517796503
    },
    20.6: {
        'target_loss_percent': 20.6,
        'actual_margin_needed': 2500.17894,
        'margin_ratio': 134.73572078720983
    },
    20.700000000000003: {
        'target_loss_percent': 20.700000000000003,
        'actual_margin_needed': 2524.137361,
        'margin_ratio': 136.02685042225846
    },
    20.8: {
        'target_loss_percent': 20.8,
        'actual_margin_needed': 2549.013808,
        'margin_ratio': 137.3674528741653
    },
    20.900000000000002: {
        'target_loss_percent': 20.900000000000002,
        'actual_margin_needed': 2573.231995,
        'margin_ratio': 138.67258141092694
    },
    21.0: {
        'target_loss_percent': 21.0,
        'actual_margin_needed': 2597.570263,
        'margin_ratio': 139.98418116453988
    },
    21.1: {
        'target_loss_percent': 21.1,
        'actual_margin_needed': 2622.028692,
        'margin_ratio': 141.30225644623863
    },
    21.200000000000003: {
        'target_loss_percent': 21.200000000000003,
        'actual_margin_needed': 2647.406356,
        'margin_ratio': 142.66986969832675
    },
    21.3: {
        'target_loss_percent': 21.3,
        'actual_margin_needed': 2672.12575,
        'margin_ratio': 144.00200849636536
    },
    21.400000000000002: {
        'target_loss_percent': 21.400000000000002,
        'actual_margin_needed': 2696.965624,
        'margin_ratio': 145.34064001353727
    },
    21.5: {
        'target_loss_percent': 21.5,
        'actual_margin_needed': 2721.926059,
        'margin_ratio': 146.6857686149674
    },
    21.6: {
        'target_loss_percent': 21.6,
        'actual_margin_needed': 2747.808925,
        'margin_ratio': 148.08060742060457
    },
    21.700000000000003: {
        'target_loss_percent': 21.700000000000003,
        'actual_margin_needed': 2773.031516,
        'margin_ratio': 149.43986372187794
    },
    21.8: {
        'target_loss_percent': 21.8,
        'actual_margin_needed': 2798.374985,
        'margin_ratio': 150.8056341906762
    },
    21.900000000000002: {
        'target_loss_percent': 21.900000000000002,
        'actual_margin_needed': 2824.624871,
        'margin_ratio': 152.22025186231855
    },
    22.0: {
        'target_loss_percent': 22.0,
        'actual_margin_needed': 2850.231446,
        'margin_ratio': 153.60020122686953
    },
    22.1: {
        'target_loss_percent': 22.1,
        'actual_margin_needed': 2875.959214,
        'margin_ratio': 154.9866817344312
    },
    22.200000000000003: {
        'target_loss_percent': 22.200000000000003,
        'actual_margin_needed': 2901.808255,
        'margin_ratio': 156.37969769623794
    },
    22.3: {
        'target_loss_percent': 22.3,
        'actual_margin_needed': 2928.570468,
        'margin_ratio': 157.8219248907506
    },
    22.400000000000002: {
        'target_loss_percent': 22.400000000000002,
        'actual_margin_needed': 2954.683798,
        'margin_ratio': 159.22918350069006
    },
    22.5: {
        'target_loss_percent': 22.5,
        'actual_margin_needed': 2980.918714,
        'margin_ratio': 160.64299443257954
    },
    22.6: {
        'target_loss_percent': 22.6,
        'actual_margin_needed': 3008.05362,
        'margin_ratio': 162.1053062135128
    },
    22.700000000000003: {
        'target_loss_percent': 22.700000000000003,
        'actual_margin_needed': 3034.553766,
        'margin_ratio': 163.53341050443058
    },
    22.8: {
        'target_loss_percent': 22.8,
        'actual_margin_needed': 3061.175812,
        'margin_ratio': 164.96808403889378
    },
    22.900000000000002: {
        'target_loss_percent': 22.900000000000002,
        'actual_margin_needed': 3087.919837,
        'margin_ratio': 166.40933107424644
    },
    23.0: {
        'target_loss_percent': 23.0,
        'actual_margin_needed': 3115.574131,
        'margin_ratio': 167.89963289838363
    },
    23.1: {
        'target_loss_percent': 23.1,
        'actual_margin_needed': 3142.584557,
        'margin_ratio': 169.35523639845937
    },
    23.200000000000003: {
        'target_loss_percent': 23.200000000000003,
        'actual_margin_needed': 3169.717272,
        'margin_ratio': 170.81743010545816
    },
    23.3: {
        'target_loss_percent': 23.3,
        'actual_margin_needed': 3197.749883,
        'margin_ratio': 172.3281195958002
    },
    23.400000000000002: {
        'target_loss_percent': 23.400000000000002,
        'actual_margin_needed': 3225.149933,
        'margin_ratio': 173.80472010118467
    },
    23.5: {
        'target_loss_percent': 23.5,
        'actual_margin_needed': 3252.672583,
        'margin_ratio': 175.28792757341625
    },
    23.6: {
        'target_loss_percent': 23.6,
        'actual_margin_needed': 3281.085992,
        'margin_ratio': 176.81913843212254
    },
    23.700000000000003: {
        'target_loss_percent': 23.700000000000003,
        'actual_margin_needed': 3308.876906,
        'margin_ratio': 178.31680276695027
    },
    23.8: {
        'target_loss_percent': 23.8,
        'actual_margin_needed': 3336.790729,
        'margin_ratio': 179.82109072076832
    },
    23.900000000000002: {
        'target_loss_percent': 23.900000000000002,
        'actual_margin_needed': 3364.827539,
        'margin_ratio': 181.33200649703033
    },
    24.0: {
        'target_loss_percent': 24.0,
        'actual_margin_needed': 3393.770424,
        'margin_ratio': 182.89175104560906
    },
    24.1: {
        'target_loss_percent': 24.1,
        'actual_margin_needed': 3422.076653,
        'margin_ratio': 184.41718592791506
    },
    24.200000000000003: {
        'target_loss_percent': 24.200000000000003,
        'actual_margin_needed': 3450.506177,
        'margin_ratio': 185.9492652309178
    },
    24.3: {
        'target_loss_percent': 24.3,
        'actual_margin_needed': 3479.835411,
        'margin_ratio': 187.52983029364356
    },
    24.400000000000002: {
        'target_loss_percent': 24.400000000000002,
        'actual_margin_needed': 3508.535276,
        'margin_ratio': 189.07647838966767
    },
    24.5: {
        'target_loss_percent': 24.5,
        'actual_margin_needed': 3537.358741,
        'margin_ratio': 190.62978734297
    },
    24.6: {
        'target_loss_percent': 24.6,
        'actual_margin_needed': 3567.076776,
        'margin_ratio': 192.23130505917976
    },
    24.700000000000003: {
        'target_loss_percent': 24.700000000000003,
        'actual_margin_needed': 3596.171499,
        'margin_ratio': 193.79923222302878
    },
    24.8: {
        'target_loss_percent': 24.8,
        'actual_margin_needed': 3625.390128,
        'margin_ratio': 195.37383673462787
    },
    24.900000000000002: {
        'target_loss_percent': 24.900000000000002,
        'actual_margin_needed': 3655.499405,
        'margin_ratio': 196.9964386508638
    },
    25.0: {
        'target_loss_percent': 25.0,
        'actual_margin_needed': 3684.990205,
        'margin_ratio': 198.58571057497315
    },
    25.1: {
        'target_loss_percent': 25.1,
        'actual_margin_needed': 3714.605215,
        'margin_ratio': 200.18167622952365
    },
    25.200000000000003: {
        'target_loss_percent': 25.200000000000003,
        'actual_margin_needed': 3745.108166,
        'margin_ratio': 201.8254934073141
    },
    25.3: {
        'target_loss_percent': 25.3,
        'actual_margin_needed': 3774.996257,
        'margin_ratio': 203.43617551466707
    },
    25.400000000000002: {
        'target_loss_percent': 25.400000000000002,
        'actual_margin_needed': 3805.008861,
        'margin_ratio': 205.0535676812618
    },
    25.5: {
        'target_loss_percent': 25.5,
        'actual_margin_needed': 3835.907906,
        'margin_ratio': 206.7187305354499
    },
    25.6: {
        'target_loss_percent': 25.6,
        'actual_margin_needed': 3866.194498,
        'margin_ratio': 208.3508880334681
    },
    25.700000000000003: {
        'target_loss_percent': 25.700000000000003,
        'actual_margin_needed': 3896.605904,
        'margin_ratio': 209.98977181174777
    },
    25.8: {
        'target_loss_percent': 25.8,
        'actual_margin_needed': 3927.903456,
        'margin_ratio': 211.6764103799437
    },
    25.900000000000002: {
        'target_loss_percent': 25.900000000000002,
        'actual_margin_needed': 3958.589752,
        'margin_ratio': 213.33010809881563
    },
    26.0: {
        'target_loss_percent': 26.0,
        'actual_margin_needed': 3989.401162,
        'margin_ratio': 214.9905482650784
    },
    26.1: {
        'target_loss_percent': 26.1,
        'actual_margin_needed': 4021.099624,
        'margin_ratio': 216.69879204598791
    },
    26.200000000000003: {
        'target_loss_percent': 26.200000000000003,
        'actual_margin_needed': 4052.186824,
        'margin_ratio': 218.37409465423087
    },
    26.3: {
        'target_loss_percent': 26.3,
        'actual_margin_needed': 4084.137538,
        'margin_ratio': 220.0959324041545
    },
    26.400000000000002: {
        'target_loss_percent': 26.400000000000002,
        'actual_margin_needed': 4115.5012,
        'margin_ratio': 221.78613366874734
    },
    26.5: {
        'target_loss_percent': 26.5,
        'actual_margin_needed': 4146.990499,
        'margin_ratio': 223.48310556542648
    },
    26.6: {
        'target_loss_percent': 26.6,
        'actual_margin_needed': 4179.346303,
        'margin_ratio': 225.22677379006552
    },
    26.700000000000003: {
        'target_loss_percent': 26.700000000000003,
        'actual_margin_needed': 4211.112957,
        'margin_ratio': 226.93869246724944
    },
    26.8: {
        'target_loss_percent': 26.8,
        'actual_margin_needed': 4243.005544,
        'margin_ratio': 228.65739772808718
    },
    26.900000000000002: {
        'target_loss_percent': 26.900000000000002,
        'actual_margin_needed': 4275.768814,
        'margin_ratio': 230.4230244711058
    },
    27.0: {
        'target_loss_percent': 27.0,
        'actual_margin_needed': 4307.939645,
        'margin_ratio': 232.15672442104162
    },
    27.1: {
        'target_loss_percent': 27.1,
        'actual_margin_needed': 4340.236706,
        'margin_ratio': 233.89722696008926
    },
    27.200000000000003: {
        'target_loss_percent': 27.200000000000003,
        'actual_margin_needed': 4373.409809,
        'margin_ratio': 235.6849397801378
    },
    27.3: {
        'target_loss_percent': 27.3,
        'actual_margin_needed': 4405.985999,
        'margin_ratio': 237.44048470131497
    },
    27.400000000000002: {
        'target_loss_percent': 27.400000000000002,
        'actual_margin_needed': 4439.418024,
        'margin_ratio': 239.2421509395527
    },
    27.5: {
        'target_loss_percent': 27.5,
        'actual_margin_needed': 4472.274006,
        'margin_ratio': 241.01277397221514
    },
    27.6: {
        'target_loss_percent': 27.6,
        'actual_margin_needed': 4505.256732,
        'margin_ratio': 242.79022729367102
    },
    27.700000000000003: {
        'target_loss_percent': 27.700000000000003,
        'actual_margin_needed': 4539.102709,
        'margin_ratio': 244.61420158362418
    },
    27.8: {
        'target_loss_percent': 27.8,
        'actual_margin_needed': 4572.366106,
        'margin_ratio': 246.40678038625427
    },
    27.900000000000002: {
        'target_loss_percent': 27.900000000000002,
        'actual_margin_needed': 4605.75654,
        'margin_ratio': 248.20620526757406
    },
    28.0: {
        'target_loss_percent': 28.0,
        'actual_margin_needed': 4640.01881,
        'margin_ratio': 250.0526137667417
    },
    28.1: {
        'target_loss_percent': 28.1,
        'actual_margin_needed': 4673.690791,
        'margin_ratio': 251.8672113372533
    },
    28.200000000000003: {
        'target_loss_percent': 28.200000000000003,
        'actual_margin_needed': 4708.21681,
        'margin_ratio': 253.72783338372085
    },
    28.3: {
        'target_loss_percent': 28.3,
        'actual_margin_needed': 4742.170993,
        'margin_ratio': 255.55763894165656
    },
    28.400000000000002: {
        'target_loss_percent': 28.400000000000002,
        'actual_margin_needed': 4776.252722,
        'margin_ratio': 257.39431800851145
    },
    28.5: {
        'target_loss_percent': 28.5,
        'actual_margin_needed': 4811.199107,
        'margin_ratio': 259.2775937599192
    },
    28.6: {
        'target_loss_percent': 28.6,
        'actual_margin_needed': 4845.563908,
        'margin_ratio': 261.1295276988732
    },
    28.700000000000003: {
        'target_loss_percent': 28.700000000000003,
        'actual_margin_needed': 4880.777088,
        'margin_ratio': 263.0271811478338
    },
    28.8: {
        'target_loss_percent': 28.8,
        'actual_margin_needed': 4915.425611,
        'margin_ratio': 264.89440498766714
    },
    28.900000000000002: {
        'target_loss_percent': 28.900000000000002,
        'actual_margin_needed': 4950.202185,
        'margin_ratio': 266.7685295510873
    },
    29.0: {
        'target_loss_percent': 29.0,
        'actual_margin_needed': 4985.839776,
        'margin_ratio': 268.68905469178173
    },
    29.1: {
        'target_loss_percent': 29.1,
        'actual_margin_needed': 5020.900936,
        'margin_ratio': 270.57851571741367
    },
    29.200000000000003: {
        'target_loss_percent': 29.200000000000003,
        'actual_margin_needed': 5056.808345,
        'margin_ratio': 272.51358146643406
    },
    29.3: {
        'target_loss_percent': 29.3,
        'actual_margin_needed': 5092.154737,
        'margin_ratio': 274.4184137674962
    },
    29.400000000000002: {
        'target_loss_percent': 29.400000000000002,
        'actual_margin_needed': 5127.629682,
        'margin_ratio': 276.3301738451415
    },
    29.5: {
        'target_loss_percent': 29.5,
        'actual_margin_needed': 5163.965519,
        'margin_ratio': 278.28832776375725
    },
    29.6: {
        'target_loss_percent': 29.6,
        'actual_margin_needed': 5199.726553,
        'margin_ratio': 280.21550530093225
    },
    29.700000000000003: {
        'target_loss_percent': 29.700000000000003,
        'actual_margin_needed': 5236.33521,
        'margin_ratio': 282.188361606948
    },
    29.8: {
        'target_loss_percent': 29.8,
        'actual_margin_needed': 5272.382975,
        'margin_ratio': 284.1309912013093
    },
    29.900000000000002: {
        'target_loss_percent': 29.900000000000002,
        'actual_margin_needed': 5309.265732,
        'margin_ratio': 286.11861887447674
    },
    30.0: {
        'target_loss_percent': 30.0,
        'actual_margin_needed': 5345.600867,
        'margin_ratio': 288.07673496200994
    },
    30.1: {
        'target_loss_percent': 30.1,
        'actual_margin_needed': 5382.065266,
        'margin_ratio': 290.0418171422227
    },
    30.200000000000003: {
        'target_loss_percent': 30.200000000000003,
        'actual_margin_needed': 5419.382142,
        'margin_ratio': 292.0528396754287
    },
    30.3: {
        'target_loss_percent': 30.3,
        'actual_margin_needed': 5456.134759,
        'margin_ratio': 294.03345404789883
    },
    30.400000000000002: {
        'target_loss_percent': 30.400000000000002,
        'actual_margin_needed': 5493.728708,
        'margin_ratio': 296.05940816450806
    },
    30.5: {
        'target_loss_percent': 30.5,
        'actual_margin_needed': 5530.770179,
        'margin_ratio': 298.05558900354976
    },
    30.6: {
        'target_loss_percent': 30.6,
        'actual_margin_needed': 5568.642468,
        'margin_ratio': 300.09654298273836
    },
    30.700000000000003: {
        'target_loss_percent': 30.700000000000003,
        'actual_margin_needed': 5605.973425,
        'margin_ratio': 302.1083243471039
    },
    30.8: {
        'target_loss_percent': 30.8,
        'actual_margin_needed': 5643.434352,
        'margin_ratio': 304.12710985079354
    },
    30.900000000000002: {
        'target_loss_percent': 30.900000000000002,
        'actual_margin_needed': 5681.746392,
        'margin_ratio': 306.1917622009283
    },
    31.0: {
        'target_loss_percent': 31.0,
        'actual_margin_needed': 5719.497646,
        'margin_ratio': 308.2261935517943
    },
    31.1: {
        'target_loss_percent': 31.1,
        'actual_margin_needed': 5758.090969,
        'margin_ratio': 310.30600436404706
    },
    31.200000000000003: {
        'target_loss_percent': 31.200000000000003,
        'actual_margin_needed': 5796.133178,
        'margin_ratio': 312.35611540528026
    },
    31.3: {
        'target_loss_percent': 31.3,
        'actual_margin_needed': 5835.009039,
        'margin_ratio': 314.45115231214197
    },
    31.400000000000002: {
        'target_loss_percent': 31.400000000000002,
        'actual_margin_needed': 5873.34283,
        'margin_ratio': 316.5169768330425
    },
    31.5: {
        'target_loss_percent': 31.5,
        'actual_margin_needed': 5912.50248,
        'margin_ratio': 318.6273072514425
    },
    31.6: {
        'target_loss_percent': 31.6,
        'actual_margin_needed': 5951.128477,
        'margin_ratio': 320.7088788796395
    },
    31.700000000000003: {
        'target_loss_percent': 31.700000000000003,
        'actual_margin_needed': 5990.573163,
        'margin_ratio': 322.83457001094513
    },
    31.8: {
        'target_loss_percent': 31.8,
        'actual_margin_needed': 6029.49199,
        'margin_ratio': 324.9319223740675
    },
    31.900000000000002: {
        'target_loss_percent': 31.900000000000002,
        'actual_margin_needed': 6069.222956,
        'margin_ratio': 327.07304125797504
    },
    32.0: {
        'target_loss_percent': 32.0,
        'actual_margin_needed': 6108.435233,
        'margin_ratio': 329.18620776809007
    },
    32.1: {
        'target_loss_percent': 32.1,
        'actual_margin_needed': 6147.778798,
        'margin_ratio': 331.30644944511715
    },
    32.2: {
        'target_loss_percent': 32.2,
        'actual_margin_needed': 6187.960066,
        'margin_ratio': 333.47183529790897
    },
    32.3: {
        'target_loss_percent': 32.3,
        'actual_margin_needed': 6227.597905,
        'margin_ratio': 335.60793552764386
    },
    32.400000000000006: {
        'target_loss_percent': 32.400000000000006,
        'actual_margin_needed': 6268.06834,
        'margin_ratio': 337.7889047147121
    },
    32.5: {
        'target_loss_percent': 32.5,
        'actual_margin_needed': 6308.001072,
        'margin_ratio': 339.94089685533163
    },
    32.6: {
        'target_loss_percent': 32.6,
        'actual_margin_needed': 6348.761905,
        'margin_ratio': 342.13751571579695
    },
    32.7: {
        'target_loss_percent': 32.7,
        'actual_margin_needed': 6388.990143,
        'margin_ratio': 344.30543280213533
    },
    32.8: {
        'target_loss_percent': 32.8,
        'actual_margin_needed': 6430.042603,
        'margin_ratio': 346.5177675673374
    },
    32.900000000000006: {
        'target_loss_percent': 32.900000000000006,
        'actual_margin_needed': 6470.56696,
        'margin_ratio': 348.7016426342289
    },
    33.0: {
        'target_loss_percent': 33.0,
        'actual_margin_needed': 6511.91227,
        'margin_ratio': 350.929759212165
    },
    33.1: {
        'target_loss_percent': 33.1,
        'actual_margin_needed': 6552.733358,
        'margin_ratio': 353.1296252405534
    },
    33.2: {
        'target_loss_percent': 33.2,
        'actual_margin_needed': 6594.372737,
        'margin_ratio': 355.37358932365885
    },
    33.300000000000004: {
        'target_loss_percent': 33.300000000000004,
        'actual_margin_needed': 6635.491165,
        'margin_ratio': 357.58947913281673
    },
    33.4: {
        'target_loss_percent': 33.4,
        'actual_margin_needed': 6677.425831,
        'margin_ratio': 359.84935635963666
    },
    33.5: {
        'target_loss_percent': 33.5,
        'actual_margin_needed': 6718.842205,
        'margin_ratio': 362.0813026071651
    },
    33.6: {
        'target_loss_percent': 33.6,
        'actual_margin_needed': 6761.073372,
        'margin_ratio': 364.35715840068286
    },
    33.7: {
        'target_loss_percent': 33.7,
        'actual_margin_needed': 6802.788299,
        'margin_ratio': 366.60519379807357
    },
    33.800000000000004: {
        'target_loss_percent': 33.800000000000004,
        'actual_margin_needed': 6845.317174,
        'margin_ratio': 368.8970932040394
    },
    33.9: {
        'target_loss_percent': 33.9,
        'actual_margin_needed': 6887.331257,
        'margin_ratio': 371.1612502472223
    },
    34.0: {
        'target_loss_percent': 34.0,
        'actual_margin_needed': 6930.159047,
        'margin_ratio': 373.46925831138645
    },
    34.1: {
        'target_loss_percent': 34.1,
        'actual_margin_needed': 6972.472888,
        'margin_ratio': 375.7495694424011
    },
    34.2: {
        'target_loss_percent': 34.2,
        'actual_margin_needed': 7015.600795,
        'margin_ratio': 378.07375094106163
    },
    34.300000000000004: {
        'target_loss_percent': 34.300000000000004,
        'actual_margin_needed': 7058.214994,
        'margin_ratio': 380.3702484941666
    },
    34.4: {
        'target_loss_percent': 34.4,
        'actual_margin_needed': 7101.644216,
        'margin_ratio': 382.71066798806
    },
    34.5: {
        'target_loss_percent': 34.5,
        'actual_margin_needed': 7144.559372,
        'margin_ratio': 385.0233842436235
    },
    34.6: {
        'target_loss_percent': 34.6,
        'actual_margin_needed': 7188.291105,
        'margin_ratio': 387.3801061857053
    },
    34.7: {
        'target_loss_percent': 34.7,
        'actual_margin_needed': 7231.507813,
        'margin_ratio': 389.7090732085338
    },
    34.800000000000004: {
        'target_loss_percent': 34.800000000000004,
        'actual_margin_needed': 7275.543247,
        'margin_ratio': 392.08216172841696
    },
    34.9: {
        'target_loss_percent': 34.9,
        'actual_margin_needed': 7319.715142,
        'margin_ratio': 394.4626041904121
    },
    35.0: {
        'target_loss_percent': 35.0,
        'actual_margin_needed': 7363.402427,
        'margin_ratio': 396.81693081061445
    },
    35.1: {
        'target_loss_percent': 35.1,
        'actual_margin_needed': 7407.88,
        'margin_ratio': 399.2138463917931
    },
    35.2: {
        'target_loss_percent': 35.2,
        'actual_margin_needed': 7451.87042,
        'margin_ratio': 401.5845090878128
    },
    35.300000000000004: {
        'target_loss_percent': 35.300000000000004,
        'actual_margin_needed': 7496.654854,
        'margin_ratio': 403.99796154055497
    },
    35.4: {
        'target_loss_percent': 35.4,
        'actual_margin_needed': 7540.949,
        'margin_ratio': 406.3849921616368
    },
    35.5: {
        'target_loss_percent': 35.5,
        'actual_margin_needed': 7586.041473,
        'margin_ratio': 408.81504496887015
    },
    35.6: {
        'target_loss_percent': 35.6,
        'actual_margin_needed': 7630.639931,
        'margin_ratio': 411.2184750948067
    },
    35.7: {
        'target_loss_percent': 35.7,
        'actual_margin_needed': 7676.041618,
        'margin_ratio': 413.6651915777878
    },
    35.800000000000004: {
        'target_loss_percent': 35.800000000000004,
        'actual_margin_needed': 7720.944976,
        'margin_ratio': 416.08505289615255
    },
    35.9: {
        'target_loss_percent': 35.9,
        'actual_margin_needed': 7766.657048,
        'margin_ratio': 418.5484961605762
    },
    36.0: {
        'target_loss_percent': 36.0,
        'actual_margin_needed': 7812.510025,
        'margin_ratio': 421.0195328561874
    },
    36.1: {
        'target_loss_percent': 36.1,
        'actual_margin_needed': 7857.889515,
        'margin_ratio': 423.46505313327043
    },
    36.2: {
        'target_loss_percent': 36.2,
        'actual_margin_needed': 7904.054821,
        'margin_ratio': 425.9529213733221
    },
    36.300000000000004: {
        'target_loss_percent': 36.300000000000004,
        'actual_margin_needed': 7949.740766,
        'margin_ratio': 428.41495664245366
    },
    36.4: {
        'target_loss_percent': 36.4,
        'actual_margin_needed': 7996.219563,
        'margin_ratio': 430.9197190476267
    },
    36.5: {
        'target_loss_percent': 36.5,
        'actual_margin_needed': 8042.212544,
        'margin_ratio': 433.3983006191471
    },
    36.6: {
        'target_loss_percent': 36.6,
        'actual_margin_needed': 8089.00599,
        'margin_ratio': 435.9200195945607
    },
    36.7: {
        'target_loss_percent': 36.7,
        'actual_margin_needed': 8135.943177,
        'margin_ratio': 438.4494848344243
    },
    36.800000000000004: {
        'target_loss_percent': 36.800000000000004,
        'actual_margin_needed': 8182.415834,
        'margin_ratio': 440.9539163523507
    },
    36.9: {
        'target_loss_percent': 36.9,
        'actual_margin_needed': 8229.669594,
        'margin_ratio': 443.5004418476442
    },
    37.0: {
        'target_loss_percent': 37.0,
        'actual_margin_needed': 8276.450824,
        'margin_ratio': 446.021502497552
    },
    37.1: {
        'target_loss_percent': 37.1,
        'actual_margin_needed': 8324.022305,
        'margin_ratio': 448.5851501144902
    },
    37.2: {
        'target_loss_percent': 37.2,
        'actual_margin_needed': 8371.112682,
        'margin_ratio': 451.1228708294869
    },
    37.300000000000004: {
        'target_loss_percent': 37.300000000000004,
        'actual_margin_needed': 8419.00303,
        'margin_ratio': 453.70370232650373
    },
    37.4: {
        'target_loss_percent': 37.4,
        'actual_margin_needed': 8467.039924,
        'margin_ratio': 456.2924312506298
    },
    37.5: {
        'target_loss_percent': 37.5,
        'actual_margin_needed': 8514.613484,
        'margin_ratio': 458.85619090577416
    },
    37.6: {
        'target_loss_percent': 37.6,
        'actual_margin_needed': 8562.971147,
        'margin_ratio': 461.46220620957877
    },
    37.7: {
        'target_loss_percent': 37.7,
        'actual_margin_needed': 8610.855376,
        'margin_ratio': 464.04270795104804
    },
    37.800000000000004: {
        'target_loss_percent': 37.800000000000004,
        'actual_margin_needed': 8659.534944,
        'margin_ratio': 466.66607085406093
    },
    37.9: {
        'target_loss_percent': 37.9,
        'actual_margin_needed': 8708.363078,
        'margin_ratio': 469.2974400428536
    },
    38.0: {
        'target_loss_percent': 38.0,
        'actual_margin_needed': 8756.733015,
        'margin_ratio': 471.90411679780897
    },
    38.1: {
        'target_loss_percent': 38.1,
        'actual_margin_needed': 8805.884942,
        'margin_ratio': 474.5529353309438
    },
    38.2: {
        'target_loss_percent': 38.2,
        'actual_margin_needed': 8855.18669,
        'margin_ratio': 477.20982778234946
    },
    38.300000000000004: {
        'target_loss_percent': 38.300000000000004,
        'actual_margin_needed': 8904.043903,
        'margin_ratio': 479.8427640509868
    },
    38.4: {
        'target_loss_percent': 38.4,
        'actual_margin_needed': 8953.671321,
        'margin_ratio': 482.51720699907355
    },
    38.5: {
        'target_loss_percent': 38.5,
        'actual_margin_needed': 9002.841649,
        'margin_ratio': 485.1670171700302
    },
    38.6: {
        'target_loss_percent': 38.6,
        'actual_margin_needed': 9052.795858,
        'margin_ratio': 487.85907102597133
    },
    38.7: {
        'target_loss_percent': 38.7,
        'actual_margin_needed': 9102.901882,
        'margin_ratio': 490.55930625770293
    },
    38.800000000000004: {
        'target_loss_percent': 38.800000000000004,
        'actual_margin_needed': 9152.56198,
        'margin_ratio': 493.2355103450766
    },
    38.9: {
        'target_loss_percent': 38.9,
        'actual_margin_needed': 9202.996658,
        'margin_ratio': 495.95345688253553
    },
    39.0: {
        'target_loss_percent': 39.0,
        'actual_margin_needed': 9253.58439,
        'margin_ratio': 498.67965156602895
    },
    39.1: {
        'target_loss_percent': 39.1,
        'actual_margin_needed': 9303.735806,
        'margin_ratio': 501.3823329922069
    },
    39.2: {
        'target_loss_percent': 39.2,
        'actual_margin_needed': 9354.654044,
        'margin_ratio': 504.12633878650604
    },
    39.300000000000004: {
        'target_loss_percent': 39.300000000000004,
        'actual_margin_needed': 9405.12099,
        'margin_ratio': 506.8460242603942
    },
    39.400000000000006: {
        'target_loss_percent': 39.400000000000006,
        'actual_margin_needed': 9456.370842,
        'margin_ratio': 509.6079008760967
    },
    39.5: {
        'target_loss_percent': 39.5,
        'actual_margin_needed': 9507.775718,
        'margin_ratio': 512.3781318019828
    },
    39.6: {
        'target_loss_percent': 39.6,
        'actual_margin_needed': 9558.736442,
        'margin_ratio': 515.1244271851357
    },
    39.7: {
        'target_loss_percent': 39.7,
        'actual_margin_needed': 9610.474769,
        'margin_ratio': 517.912627929147
    },
    39.800000000000004: {
        'target_loss_percent': 39.800000000000004,
        'actual_margin_needed': 9662.369342,
        'margin_ratio': 520.7092488374486
    },
    39.900000000000006: {
        'target_loss_percent': 39.900000000000006,
        'actual_margin_needed': 9713.825373,
        'margin_ratio': 523.4822365283353
    },
    40.0: {
        'target_loss_percent': 40.0,
        'actual_margin_needed': 9766.055227,
        'margin_ratio': 526.296925874251
    },
    40.1: {
        'target_loss_percent': 40.1,
        'actual_margin_needed': 9818.442542,
        'margin_ratio': 529.1201008613308
    },
    40.2: {
        'target_loss_percent': 40.2,
        'actual_margin_needed': 9870.3954,
        'margin_ratio': 531.9198627734063
    },
    40.300000000000004: {
        'target_loss_percent': 40.300000000000004,
        'actual_margin_needed': 9923.119817,
        'margin_ratio': 534.7612043325751
    },
    40.400000000000006: {
        'target_loss_percent': 40.400000000000006,
        'actual_margin_needed': 9976.002905,
        'margin_ratio': 537.6110967403295
    },
    40.5: {
        'target_loss_percent': 40.5,
        'actual_margin_needed': 10028.454102,
        'margin_ratio': 540.4377143559258
    },
    40.6: {
        'target_loss_percent': 40.6,
        'actual_margin_needed': 10081.676103,
        'margin_ratio': 543.3058709313398
    },
    40.7: {
        'target_loss_percent': 40.7,
        'actual_margin_needed': 10135.05798,
        'margin_ratio': 546.1826432933088
    },
    40.800000000000004: {
        'target_loss_percent': 40.800000000000004,
        'actual_margin_needed': 10188.009022,
        'margin_ratio': 549.0361977714151
    },
    40.900000000000006: {
        'target_loss_percent': 40.900000000000006,
        'actual_margin_needed': 10241.731613,
        'margin_ratio': 551.9313313577101
    },
    41.0: {
        'target_loss_percent': 41.0,
        'actual_margin_needed': 10295.615278,
        'margin_ratio': 554.8351452912966
    },
    41.1: {
        'target_loss_percent': 41.1,
        'actual_margin_needed': 10349.660495,
        'margin_ratio': 557.7476653318006
    },
    41.2: {
        'target_loss_percent': 41.2,
        'actual_margin_needed': 10403.293835,
        'margin_ratio': 560.6379891432338
    },
    41.300000000000004: {
        'target_loss_percent': 41.300000000000004,
        'actual_margin_needed': 10457.682273,
        'margin_ratio': 563.5690055113744
    },
    41.400000000000006: {
        'target_loss_percent': 41.400000000000006,
        'actual_margin_needed': 10512.233453,
        'margin_ratio': 566.5087921160455
    },
    41.5: {
        'target_loss_percent': 41.5,
        'actual_margin_needed': 10566.37022,
        'margin_ratio': 569.4262458255125
    },
    41.6: {
        'target_loss_percent': 41.6,
        'actual_margin_needed': 10621.266402,
        'margin_ratio': 572.3846247366778
    },
    41.7: {
        'target_loss_percent': 41.7,
        'actual_margin_needed': 10676.326509,
        'margin_ratio': 575.3518376367537
    },
    41.800000000000004: {
        'target_loss_percent': 41.800000000000004,
        'actual_margin_needed': 10731.551015,
        'margin_ratio': 578.3279100698041
    },
    41.900000000000006: {
        'target_loss_percent': 41.900000000000006,
        'actual_margin_needed': 10786.375064,
        'margin_ratio': 581.2824017025065
    },
    42.0: {
        'target_loss_percent': 42.0,
        'actual_margin_needed': 10841.947048,
        'margin_ratio': 584.2771998747586
    },
    42.1: {
        'target_loss_percent': 42.1,
        'actual_margin_needed': 10897.684606,
        'margin_ratio': 587.280920901242
    },
    42.2: {
        'target_loss_percent': 42.2,
        'actual_margin_needed': 10953.015621,
        'margin_ratio': 590.2627331502136
    },
    42.300000000000004: {
        'target_loss_percent': 42.300000000000004,
        'actual_margin_needed': 11009.102418,
        'margin_ratio': 593.2852748169477
    },
    42.400000000000006: {
        'target_loss_percent': 42.400000000000006,
        'actual_margin_needed': 11065.355957,
        'margin_ratio': 596.3168022819364
    },
    42.5: {
        'target_loss_percent': 42.5,
        'actual_margin_needed': 11121.776706,
        'margin_ratio': 599.3573407659015
    },
    42.6: {
        'target_loss_percent': 42.6,
        'actual_margin_needed': 11177.799929,
        'margin_ratio': 602.3764564023718
    },
    42.7: {
        'target_loss_percent': 42.7,
        'actual_margin_needed': 11234.572364,
        'margin_ratio': 605.4359473964723
    },
    42.800000000000004: {
        'target_loss_percent': 42.800000000000004,
        'actual_margin_needed': 11291.51317,
        'margin_ratio': 608.5045119763399
    },
    42.900000000000006: {
        'target_loss_percent': 42.900000000000006,
        'actual_margin_needed': 11348.62281,
        'margin_ratio': 611.5821750932439
    },
    43.0: {
        'target_loss_percent': 43.0,
        'actual_margin_needed': 11405.341087,
        'margin_ratio': 614.6387474893795
    },
    43.1: {
        'target_loss_percent': 43.1,
        'actual_margin_needed': 11462.804844,
        'margin_ratio': 617.7354941240568
    },
    43.2: {
        'target_loss_percent': 43.2,
        'actual_margin_needed': 11520.438589,
        'margin_ratio': 620.841401485328
    },
    43.300000000000004: {
        'target_loss_percent': 43.300000000000004,
        'actual_margin_needed': 11578.242781,
        'margin_ratio': 623.956494308901
    },
    43.400000000000006: {
        'target_loss_percent': 43.400000000000006,
        'actual_margin_needed': 11635.658938,
        'margin_ratio': 627.0506757590601
    },
    43.5: {
        'target_loss_percent': 43.5,
        'actual_margin_needed': 11693.819661,
        'margin_ratio': 630.184982191907
    },
    43.6: {
        'target_loss_percent': 43.6,
        'actual_margin_needed': 11752.151977,
        'margin_ratio': 633.3285358454896
    },
    43.7: {
        'target_loss_percent': 43.7,
        'actual_margin_needed': 11810.656343,
        'margin_ratio': 636.481361347735
    },
    43.800000000000004: {
        'target_loss_percent': 43.800000000000004,
        'actual_margin_needed': 11868.773186,
        'margin_ratio': 639.6133030684672
    },
    43.900000000000006: {
        'target_loss_percent': 43.900000000000006,
        'actual_margin_needed': 11927.63648,
        'margin_ratio': 642.7854713553496
    },
    44.0: {
        'target_loss_percent': 44.0,
        'actual_margin_needed': 11986.67296,
        'margin_ratio': 645.9669727104246
    },
    44.1: {
        'target_loss_percent': 44.1,
        'actual_margin_needed': 12045.883081,
        'margin_ratio': 649.157831653838
    },
    44.2: {
        'target_loss_percent': 44.2,
        'actual_margin_needed': 12105.267295,
        'margin_ratio': 652.3580725440648
    },
    44.300000000000004: {
        'target_loss_percent': 44.300000000000004,
        'actual_margin_needed': 12164.274827,
        'margin_ratio': 655.5380138789416
    },
    44.400000000000006: {
        'target_loss_percent': 44.400000000000006,
        'actual_margin_needed': 12224.021026,
        'margin_ratio': 658.7577622968532
    },
    44.5: {
        'target_loss_percent': 44.5,
        'actual_margin_needed': 12283.942446,
        'margin_ratio': 661.9869534499844
    },
    44.6: {
        'target_loss_percent': 44.6,
        'actual_margin_needed': 12344.039534,
        'margin_ratio': 665.225611427358
    },
    44.7: {
        'target_loss_percent': 44.7,
        'actual_margin_needed': 12404.312741,
        'margin_ratio': 668.4737605335583
    },
    44.800000000000004: {
        'target_loss_percent': 44.800000000000004,
        'actual_margin_needed': 12464.215528,
        'margin_ratio': 671.7019475462878
    },
    44.900000000000006: {
        'target_loss_percent': 44.900000000000006,
        'actual_margin_needed': 12524.85375,
        'margin_ratio': 674.969767468179
    },
    45.0: {
        'target_loss_percent': 45.0,
        'actual_margin_needed': 12585.669207,
        'margin_ratio': 678.247138660618
    },
    45.1: {
        'target_loss_percent': 45.1,
        'actual_margin_needed': 12646.662344,
        'margin_ratio': 681.5340851048464
    },
    45.2: {
        'target_loss_percent': 45.2,
        'actual_margin_needed': 12707.833606,
        'margin_ratio': 684.8306307821064
    },
    45.300000000000004: {
        'target_loss_percent': 45.300000000000004,
        'actual_margin_needed': 12769.183436,
        'margin_ratio': 688.1367995658586
    },
    45.400000000000006: {
        'target_loss_percent': 45.400000000000006,
        'actual_margin_needed': 12830.175453,
        'margin_ratio': 691.4236856528044
    },
    45.5: {
        'target_loss_percent': 45.5,
        'actual_margin_needed': 12891.893965,
        'margin_ratio': 694.7497228684582
    },
    45.6: {
        'target_loss_percent': 45.6,
        'actual_margin_needed': 12953.79215,
        'margin_ratio': 698.0854427395308
    },
    45.7: {
        'target_loss_percent': 45.7,
        'actual_margin_needed': 13015.87045,
        'margin_ratio': 701.4308690855925
    },
    45.800000000000004: {
        'target_loss_percent': 45.800000000000004,
        'actual_margin_needed': 13078.129302,
        'margin_ratio': 704.7860254567618
    },
    45.900000000000006: {
        'target_loss_percent': 45.900000000000006,
        'actual_margin_needed': 13140.569146,
        'margin_ratio': 708.1509355648283
    },
    46.0: {
        'target_loss_percent': 46.0,
        'actual_margin_needed': 13202.657077,
        'margin_ratio': 711.4968809296314
    },
    46.1: {
        'target_loss_percent': 46.1,
        'actual_margin_needed': 13265.469229,
        'margin_ratio': 714.8818548763026
    },
    46.2: {
        'target_loss_percent': 46.2,
        'actual_margin_needed': 13328.463467,
        'margin_ratio': 718.2766415160024
    },
    46.300000000000004: {
        'target_loss_percent': 46.300000000000004,
        'actual_margin_needed': 13391.640224,
        'margin_ratio': 721.6812641832878
    },
    46.400000000000006: {
        'target_loss_percent': 46.400000000000006,
        'actual_margin_needed': 13454.999937,
        'margin_ratio': 725.0957464282769
    },
    46.5: {
        'target_loss_percent': 46.5,
        'actual_margin_needed': 13518.543038,
        'margin_ratio': 728.5201115316361
    },
    46.6: {
        'target_loss_percent': 46.6,
        'actual_margin_needed': 13582.269962,
        'margin_ratio': 731.9543829357027
    },
    46.7: {
        'target_loss_percent': 46.7,
        'actual_margin_needed': 13645.652492,
        'margin_ratio': 735.3700947986572
    },
    46.800000000000004: {
        'target_loss_percent': 46.800000000000004,
        'actual_margin_needed': 13709.755958,
        'margin_ratio': 738.8246582134136
    },
    46.900000000000006: {
        'target_loss_percent': 46.900000000000006,
        'actual_margin_needed': 13774.044325,
        'margin_ratio': 742.2891860227622
    },
    47.0: {
        'target_loss_percent': 47.0,
        'actual_margin_needed': 13838.518023,
        'margin_ratio': 745.7637013995884
    },
    47.1: {
        'target_loss_percent': 47.1,
        'actual_margin_needed': 13903.17748,
        'margin_ratio': 749.2482274089965
    },
    47.2: {
        'target_loss_percent': 47.2,
        'actual_margin_needed': 13968.023126,
        'margin_ratio': 752.7427872238721
    },
    47.300000000000004: {
        'target_loss_percent': 47.300000000000004,
        'actual_margin_needed': 14033.055387,
        'margin_ratio': 756.2474038015388
    },
    47.400000000000006: {
        'target_loss_percent': 47.400000000000006,
        'actual_margin_needed': 14098.27469,
        'margin_ratio': 759.7621001532104
    },
    47.5: {
        'target_loss_percent': 47.5,
        'actual_margin_needed': 14163.68146,
        'margin_ratio': 763.2868991823204
    },
    47.6: {
        'target_loss_percent': 47.6,
        'actual_margin_needed': 14228.755279,
        'margin_ratio': 766.7937553385208
    },
    47.7: {
        'target_loss_percent': 47.7,
        'actual_margin_needed': 14294.544048,
        'margin_ratio': 770.3391404584027
    },
    47.800000000000004: {
        'target_loss_percent': 47.800000000000004,
        'actual_margin_needed': 14360.521344,
        'margin_ratio': 773.8946853795799
    },
    47.900000000000006: {
        'target_loss_percent': 47.900000000000006,
        'actual_margin_needed': 14426.687592,
        'margin_ratio': 777.4604130054854
    },
    48.0: {
        'target_loss_percent': 48.0,
        'actual_margin_needed': 14493.043213,
        'margin_ratio': 781.036346023991
    },
    48.1: {
        'target_loss_percent': 48.1,
        'actual_margin_needed': 14559.588628,
        'margin_ratio': 784.6225071229676
    },
    48.2: {
        'target_loss_percent': 48.2,
        'actual_margin_needed': 14626.324256,
        'margin_ratio': 788.2189188825064
    },
    48.300000000000004: {
        'target_loss_percent': 48.300000000000004,
        'actual_margin_needed': 14693.250518,
        'margin_ratio': 791.8256039904786
    },
    48.400000000000006: {
        'target_loss_percent': 48.400000000000006,
        'actual_margin_needed': 14760.367833,
        'margin_ratio': 795.4425850269748
    },
    48.5: {
        'target_loss_percent': 48.5,
        'actual_margin_needed': 14827.676617,
        'margin_ratio': 799.0698844104144
    },
    48.6: {
        'target_loss_percent': 48.6,
        'actual_margin_needed': 14895.177287,
        'margin_ratio': 802.7075246131071
    },
    48.7: {
        'target_loss_percent': 48.7,
        'actual_margin_needed': 14962.87026,
        'margin_ratio': 806.3555281073626
    },
    48.800000000000004: {
        'target_loss_percent': 48.800000000000004,
        'actual_margin_needed': 15030.755951,
        'margin_ratio': 810.0139172577099
    },
    48.900000000000006: {
        'target_loss_percent': 48.900000000000006,
        'actual_margin_needed': 15098.834774,
        'margin_ratio': 813.6827143747875
    },
    49.0: {
        'target_loss_percent': 49.0,
        'actual_margin_needed': 15167.107142,
        'margin_ratio': 817.3619417153432
    },
    49.1: {
        'target_loss_percent': 49.1,
        'actual_margin_needed': 15235.066035,
        'margin_ratio': 821.0242757530244
    },
    49.2: {
        'target_loss_percent': 49.2,
        'actual_margin_needed': 15303.729496,
        'margin_ratio': 824.7245792639321
    },
    49.300000000000004: {
        'target_loss_percent': 49.300000000000004,
        'actual_margin_needed': 15372.587532,
        'margin_ratio': 828.435368505462
    },
    49.400000000000006: {
        'target_loss_percent': 49.400000000000006,
        'actual_margin_needed': 15441.640555,
        'margin_ratio': 832.1566656804717
    },
    49.5: {
        'target_loss_percent': 49.5,
        'actual_margin_needed': 15510.888974,
        'margin_ratio': 835.8884928301475
    },
    49.6: {
        'target_loss_percent': 49.6,
        'actual_margin_needed': 15580.333198,
        'margin_ratio': 839.6308719956758
    },
    49.7: {
        'target_loss_percent': 49.7,
        'actual_margin_needed': 15649.973636,
        'margin_ratio': 843.3838252182427
    },
    49.800000000000004: {
        'target_loss_percent': 49.800000000000004,
        'actual_margin_needed': 15719.810695,
        'margin_ratio': 847.1473744312538
    },
    49.900000000000006: {
        'target_loss_percent': 49.900000000000006,
        'actual_margin_needed': 15789.844781,
        'margin_ratio': 850.921541514224
    },
    50.0: {
        'target_loss_percent': 50.0,
        'actual_margin_needed': 15860.076301,
        'margin_ratio': 854.7063484005588
    },
    50.1: {
        'target_loss_percent': 50.1,
        'actual_margin_needed': 15930.505658,
        'margin_ratio': 858.501816808102
    },
    50.2: {
        'target_loss_percent': 50.2,
        'actual_margin_needed': 16001.133258,
        'margin_ratio': 862.3079686163685
    },
    50.300000000000004: {
        'target_loss_percent': 50.300000000000004,
        'actual_margin_needed': 16071.959503,
        'margin_ratio': 866.1248254893115
    },
    50.400000000000006: {
        'target_loss_percent': 50.400000000000006,
        'actual_margin_needed': 16142.984796,
        'margin_ratio': 869.952409144775
    },
    50.5: {
        'target_loss_percent': 50.5,
        'actual_margin_needed': 16214.209539,
        'margin_ratio': 873.790741246712
    },
    50.6: {
        'target_loss_percent': 50.6,
        'actual_margin_needed': 16285.634133,
        'margin_ratio': 877.6398434051854
    },
    50.7: {
        'target_loss_percent': 50.7,
        'actual_margin_needed': 16357.258977,
        'margin_ratio': 881.4997371224772
    },
    50.800000000000004: {
        'target_loss_percent': 50.800000000000004,
        'actual_margin_needed': 16429.084472,
        'margin_ratio': 885.3704440086503
    },
    50.900000000000006: {
        'target_loss_percent': 50.900000000000006,
        'actual_margin_needed': 16501.111015,
        'margin_ratio': 889.2519854582059
    },
    51.0: {
        'target_loss_percent': 51.0,
        'actual_margin_needed': 16573.339004,
        'margin_ratio': 893.1443829195357
    },
    51.1: {
        'target_loss_percent': 51.1,
        'actual_margin_needed': 16646.259567,
        'margin_ratio': 897.0741034922615
    },
    51.2: {
        'target_loss_percent': 51.2,
        'actual_margin_needed': 16718.892907,
        'margin_ratio': 900.9883454937091
    },
    51.300000000000004: {
        'target_loss_percent': 51.300000000000004,
        'actual_margin_needed': 16791.729078,
        'margin_ratio': 904.9135181451775
    },
    51.400000000000006: {
        'target_loss_percent': 51.400000000000006,
        'actual_margin_needed': 16864.768476,
        'margin_ratio': 908.8496427872777
    },
    51.5: {
        'target_loss_percent': 51.5,
        'actual_margin_needed': 16938.011493,
        'margin_ratio': 912.7967405450586
    },
    51.6: {
        'target_loss_percent': 51.6,
        'actual_margin_needed': 17011.458522,
        'margin_ratio': 916.7548325974594
    },
    51.7: {
        'target_loss_percent': 51.7,
        'actual_margin_needed': 17085.109955,
        'margin_ratio': 920.7239400695294
    },
    51.800000000000004: {
        'target_loss_percent': 51.800000000000004,
        'actual_margin_needed': 17158.966182,
        'margin_ratio': 924.7040839785365
    },
    51.900000000000006: {
        'target_loss_percent': 51.900000000000006,
        'actual_margin_needed': 17233.027595,
        'margin_ratio': 928.6952854495296
    },
    52.0: {
        'target_loss_percent': 52.0,
        'actual_margin_needed': 17307.294582,
        'margin_ratio': 932.6975653919961
    },
    52.1: {
        'target_loss_percent': 52.1,
        'actual_margin_needed': 17381.767532,
        'margin_ratio': 936.710944769314
    },
    52.2: {
        'target_loss_percent': 52.2,
        'actual_margin_needed': 17456.446833,
        'margin_ratio': 940.7354444909698
    },
    52.300000000000004: {
        'target_loss_percent': 52.300000000000004,
        'actual_margin_needed': 17531.332872,
        'margin_ratio': 944.7710854125608
    },
    52.400000000000006: {
        'target_loss_percent': 52.400000000000006,
        'actual_margin_needed': 17606.426036,
        'margin_ratio': 948.8178883896838
    },
    52.5: {
        'target_loss_percent': 52.5,
        'actual_margin_needed': 17682.204983,
        'margin_ratio': 952.9016485082858
    },
    52.6: {
        'target_loss_percent': 52.6,
        'actual_margin_needed': 17757.717642,
        'margin_ratio': 956.9710582517836
    },
    52.7: {
        'target_loss_percent': 52.7,
        'actual_margin_needed': 17833.438773,
        'margin_ratio': 961.0517026412239
    },
    52.800000000000004: {
        'target_loss_percent': 52.800000000000004,
        'actual_margin_needed': 17909.368756,
        'margin_ratio': 965.1436021549705
    },
    52.900000000000006: {
        'target_loss_percent': 52.900000000000006,
        'actual_margin_needed': 17985.507975,
        'margin_ratio': 969.246777486949
    },
    53.0: {
        'target_loss_percent': 53.0,
        'actual_margin_needed': 18061.856811,
        'margin_ratio': 973.3612491694138
    },
    53.1: {
        'target_loss_percent': 53.1,
        'actual_margin_needed': 18138.415644,
        'margin_ratio': 977.4870376807282
    },
    53.2: {
        'target_loss_percent': 53.2,
        'actual_margin_needed': 18215.184855,
        'margin_ratio': 981.6241635531469
    },
    53.300000000000004: {
        'target_loss_percent': 53.300000000000004,
        'actual_margin_needed': 18292.633957,
        'margin_ratio': 985.7979290446249
    },
    53.400000000000006: {
        'target_loss_percent': 53.400000000000006,
        'actual_margin_needed': 18369.830767,
        'margin_ratio': 989.9580984114716
    },
    53.5: {
        'target_loss_percent': 53.5,
        'actual_margin_needed': 18447.239278,
        'margin_ratio': 994.1296764364629
    },
    53.6: {
        'target_loss_percent': 53.6,
        'actual_margin_needed': 18524.859865,
        'margin_ratio': 998.3126833285099
    },
    53.7: {
        'target_loss_percent': 53.7,
        'actual_margin_needed': 18602.692906,
        'margin_ratio': 1002.5071394581963
    },
    53.800000000000004: {
        'target_loss_percent': 53.800000000000004,
        'actual_margin_needed': 18680.738773,
        'margin_ratio': 1006.713064872762
    },
    53.900000000000006: {
        'target_loss_percent': 53.900000000000006,
        'actual_margin_needed': 18758.997843,
        'margin_ratio': 1010.9304798888994
    },
    54.0: {
        'target_loss_percent': 54.0,
        'actual_margin_needed': 18837.937017,
        'margin_ratio': 1015.184546002758
    },
    54.1: {
        'target_loss_percent': 54.1,
        'actual_margin_needed': 18916.630723,
        'margin_ratio': 1019.4253837402867
    },
    54.2: {
        'target_loss_percent': 54.2,
        'actual_margin_needed': 18995.538934,
        'margin_ratio': 1023.6777812447287
    },
    54.300000000000004: {
        'target_loss_percent': 54.300000000000004,
        'actual_margin_needed': 19074.662022,
        'margin_ratio': 1027.9417585633241
    },
    54.400000000000006: {
        'target_loss_percent': 54.400000000000006,
        'actual_margin_needed': 19154.000356,
        'margin_ratio': 1032.2173355816421
    },
    54.5: {
        'target_loss_percent': 54.5,
        'actual_margin_needed': 19234.012251,
        'margin_ratio': 1036.5292110925907
    },
    54.6: {
        'target_loss_percent': 54.6,
        'actual_margin_needed': 19313.790314,
        'margin_ratio': 1040.8284852962654
    },
    54.7: {
        'target_loss_percent': 54.7,
        'actual_margin_needed': 19393.784913,
        'margin_ratio': 1045.1394287183184
    },
    54.800000000000004: {
        'target_loss_percent': 54.800000000000004,
        'actual_margin_needed': 19473.996415,
        'margin_ratio': 1049.4620611365383
    },
    54.900000000000006: {
        'target_loss_percent': 54.900000000000006,
        'actual_margin_needed': 19554.425185,
        'margin_ratio': 1053.7964022209324
    },
    55.0: {
        'target_loss_percent': 55.0,
        'actual_margin_needed': 19635.526517,
        'margin_ratio': 1058.1669879614165
    },
    55.1: {
        'target_loss_percent': 55.1,
        'actual_margin_needed': 19716.400054,
        'margin_ratio': 1062.5252977311588
    },
    55.2: {
        'target_loss_percent': 55.2,
        'actual_margin_needed': 19797.492136,
        'margin_ratio': 1066.895384985156
    },
    55.300000000000004: {
        'target_loss_percent': 55.300000000000004,
        'actual_margin_needed': 19878.803123,
        'margin_ratio': 1071.2772691239625
    },
    55.400000000000006: {
        'target_loss_percent': 55.400000000000006,
        'actual_margin_needed': 19960.780846,
        'margin_ratio': 1075.695083953208
    },
    55.5: {
        'target_loss_percent': 55.5,
        'actual_margin_needed': 20042.540682,
        'margin_ratio': 1080.1011567581024
    },
    55.6: {
        'target_loss_percent': 55.6,
        'actual_margin_needed': 20124.520688,
        'margin_ratio': 1084.519094619202
    },
    55.7: {
        'target_loss_percent': 55.7,
        'actual_margin_needed': 20206.721224,
        'margin_ratio': 1088.9489169370618
    },
    55.800000000000004: {
        'target_loss_percent': 55.800000000000004,
        'actual_margin_needed': 20289.586306,
        'margin_ratio': 1093.4145519253163
    },
    55.900000000000006: {
        'target_loss_percent': 55.900000000000006,
        'actual_margin_needed': 20372.239734,
        'margin_ratio': 1097.8687807882768
    },
    56.0: {
        'target_loss_percent': 56.0,
        'actual_margin_needed': 20455.114945,
        'margin_ratio': 1102.3349616327075
    },
    56.1: {
        'target_loss_percent': 56.1,
        'actual_margin_needed': 20538.212294,
        'margin_ratio': 1106.8131135897115
    },
    56.2: {
        'target_loss_percent': 56.2,
        'actual_margin_needed': 20621.975605,
        'margin_ratio': 1111.3271545259609
    },
    56.300000000000004: {
        'target_loss_percent': 56.300000000000004,
        'actual_margin_needed': 20705.529855,
        'margin_ratio': 1115.8299290748037
    },
    56.400000000000006: {
        'target_loss_percent': 56.400000000000006,
        'actual_margin_needed': 20789.307484,
        'margin_ratio': 1120.3447416142446
    },
    56.5: {
        'target_loss_percent': 56.5,
        'actual_margin_needed': 20873.308847,
        'margin_ratio': 1124.871611275387
    },
    56.6: {
        'target_loss_percent': 56.6,
        'actual_margin_needed': 20957.98116,
        'margin_ratio': 1129.4346387212447
    },
    56.7: {
        'target_loss_percent': 56.7,
        'actual_margin_needed': 21042.443394,
        'margin_ratio': 1133.9863449192371
    },
    56.800000000000004: {
        'target_loss_percent': 56.800000000000004,
        'actual_margin_needed': 21127.130592,
        'margin_ratio': 1138.5501745241609
    },
    56.900000000000006: {
        'target_loss_percent': 56.900000000000006,
        'actual_margin_needed': 21212.48396,
        'margin_ratio': 1143.1499043175397
    },
    57.0: {
        'target_loss_percent': 57.0,
        'actual_margin_needed': 21297.635095,
        'margin_ratio': 1147.738735687375
    },
    57.1: {
        'target_loss_percent': 57.1,
        'actual_margin_needed': 21383.012414,
        'margin_ratio': 1152.3397562104678
    },
    57.2: {
        'target_loss_percent': 57.2,
        'actual_margin_needed': 21469.053249,
        'margin_ratio': 1156.9765338968116
    },
    57.300000000000004: {
        'target_loss_percent': 57.300000000000004,
        'actual_margin_needed': 21554.897549,
        'margin_ratio': 1161.6027202272883
    },
    57.400000000000006: {
        'target_loss_percent': 57.400000000000006,
        'actual_margin_needed': 21640.969247,
        'margin_ratio': 1166.2411611340056
    },
    57.5: {
        'target_loss_percent': 57.5,
        'actual_margin_needed': 21727.703918,
        'margin_ratio': 1170.9153299414695
    },
    57.6: {
        'target_loss_percent': 57.6,
        'actual_margin_needed': 21814.245618,
        'margin_ratio': 1175.5790994585627
    },
    57.7: {
        'target_loss_percent': 57.7,
        'actual_margin_needed': 21901.01592,
        'margin_ratio': 1180.2551884359757
    },
    57.800000000000004: {
        'target_loss_percent': 57.800000000000004,
        'actual_margin_needed': 21988.450752,
        'margin_ratio': 1184.9670892215365
    },
    57.900000000000006: {
        'target_loss_percent': 57.900000000000006,
        'actual_margin_needed': 22075.694058,
        'margin_ratio': 1189.6686685883994
    },
    58.0: {
        'target_loss_percent': 58.0,
        'actual_margin_needed': 22163.589995,
        'margin_ratio': 1194.405418521171
    },
    58.1: {
        'target_loss_percent': 58.1,
        'actual_margin_needed': 22251.308434,
        'margin_ratio': 1199.1326030102116
    },
    58.2: {
        'target_loss_percent': 58.2,
        'actual_margin_needed': 22339.257522,
        'margin_ratio': 1203.8722172732841
    },
    58.300000000000004: {
        'target_loss_percent': 58.300000000000004,
        'actual_margin_needed': 22427.864358,
        'margin_ratio': 1208.6472778596012
    },
    58.400000000000006: {
        'target_loss_percent': 58.400000000000006,
        'actual_margin_needed': 22516.291545,
        'margin_ratio': 1213.41265708387
    },
    58.5: {
        'target_loss_percent': 58.5,
        'actual_margin_needed': 22605.367111,
        'margin_ratio': 1218.2129777319349
    },
    58.6: {
        'target_loss_percent': 58.6,
        'actual_margin_needed': 22694.274502,
        'margin_ratio': 1223.0042353567574
    },
    58.7: {
        'target_loss_percent': 58.7,
        'actual_margin_needed': 22783.414562,
        'margin_ratio': 1227.8080316142825
    },
    58.800000000000004: {
        'target_loss_percent': 58.800000000000004,
        'actual_margin_needed': 22873.211638,
        'margin_ratio': 1232.6472347472566
    },
    58.900000000000006: {
        'target_loss_percent': 58.900000000000006,
        'actual_margin_needed': 22962.834832,
        'margin_ratio': 1237.477067304298
    },
    59.0: {
        'target_loss_percent': 59.0,
        'actual_margin_needed': 23053.108171,
        'margin_ratio': 1242.341936455637
    },
    59.1: {
        'target_loss_percent': 59.1,
        'actual_margin_needed': 23143.21658,
        'margin_ratio': 1247.1979174581822
    },
    59.2: {
        'target_loss_percent': 59.2,
        'actual_margin_needed': 23233.969295,
        'margin_ratio': 1252.0886203887978
    },
    59.300000000000004: {
        'target_loss_percent': 59.300000000000004,
        'actual_margin_needed': 23324.564988,
        'margin_ratio': 1256.9708613361483
    },
    59.400000000000006: {
        'target_loss_percent': 59.400000000000006,
        'actual_margin_needed': 23415.396172,
        'margin_ratio': 1261.8657929949984
    },
    59.5: {
        'target_loss_percent': 59.5,
        'actual_margin_needed': 23506.885212,
        'margin_ratio': 1266.7961767972592
    },
    59.6: {
        'target_loss_percent': 59.6,
        'actual_margin_needed': 23598.206561,
        'margin_ratio': 1271.7175236592464
    },
    59.7: {
        'target_loss_percent': 59.7,
        'actual_margin_needed': 23690.182384,
        'margin_ratio': 1276.6741404072068
    },
    59.800000000000004: {
        'target_loss_percent': 59.800000000000004,
        'actual_margin_needed': 23781.995942,
        'margin_ratio': 1281.6220126243722
    },
    59.900000000000006: {
        'target_loss_percent': 59.900000000000006,
        'actual_margin_needed': 23874.461607,
        'margin_ratio': 1286.6050271688607
    },
    60.0: {
        'target_loss_percent': 60.0,
        'actual_margin_needed': 23966.76941,
        'margin_ratio': 1291.5795344621222
    },
    60.1: {
        'target_loss_percent': 60.1,
        'actual_margin_needed': 24059.727962,
        'margin_ratio': 1296.5891108995013
    },
    60.2: {
        'target_loss_percent': 60.2,
        'actual_margin_needed': 24152.532034,
        'margin_ratio': 1301.590362343091
    },
    60.300000000000004: {
        'target_loss_percent': 60.300000000000004,
        'actual_margin_needed': 24245.986503,
        'margin_ratio': 1306.6266639613668
    },
    60.400000000000006: {
        'target_loss_percent': 60.400000000000006,
        'actual_margin_needed': 24339.288859,
        'margin_ratio': 1311.6547681445038
    },
    60.5: {
        'target_loss_percent': 60.5,
        'actual_margin_needed': 24433.242259,
        'margin_ratio': 1316.7179573694355
    },
    60.6: {
        'target_loss_percent': 60.6,
        'actual_margin_needed': 24527.044903,
        'margin_ratio': 1321.7730222885434
    },
    60.7: {
        'target_loss_percent': 60.7,
        'actual_margin_needed': 24621.500234,
        'margin_ratio': 1326.863260791424
    },
    60.800000000000004: {
        'target_loss_percent': 60.800000000000004,
        'actual_margin_needed': 24716.198156,
        'margin_ratio': 1331.9665726278643
    },
    60.900000000000006: {
        'target_loss_percent': 60.900000000000006,
        'actual_margin_needed': 24810.765406,
        'margin_ratio': 1337.062842493898
    },
    61.0: {
        'target_loss_percent': 61.0,
        'actual_margin_needed': 24905.970026,
        'margin_ratio': 1342.1934605039723
    },
    61.1: {
        'target_loss_percent': 61.1,
        'actual_margin_needed': 25001.04273,
        'margin_ratio': 1347.316969503944
    },
    61.2: {
        'target_loss_percent': 61.2,
        'actual_margin_needed': 25096.757009,
        'margin_ratio': 1352.4750532572184
    },
    61.300000000000004: {
        'target_loss_percent': 61.300000000000004,
        'actual_margin_needed': 25192.337136,
        'margin_ratio': 1357.625907501386
    },
    61.400000000000006: {
        'target_loss_percent': 61.400000000000006,
        'actual_margin_needed': 25288.564019,
        'margin_ratio': 1362.8116157051804
    },
    61.5: {
        'target_loss_percent': 61.5,
        'actual_margin_needed': 25385.039873,
        'margin_ratio': 1368.0107410634846
    },
    61.6: {
        'target_loss_percent': 61.6,
        'actual_margin_needed': 25481.395946,
        'margin_ratio': 1373.2034113720665
    },
    61.7: {
        'target_loss_percent': 61.7,
        'actual_margin_needed': 25578.389088,
        'margin_ratio': 1378.4304136036692
    },
    61.800000000000004: {
        'target_loss_percent': 61.800000000000004,
        'actual_margin_needed': 25675.257656,
        'margin_ratio': 1383.650702488714
    },
    61.900000000000006: {
        'target_loss_percent': 61.900000000000006,
        'actual_margin_needed': 25772.770993,
        'margin_ratio': 1388.9057382530984
    },
    62.0: {
        'target_loss_percent': 62.0,
        'actual_margin_needed': 25870.537836,
        'margin_ratio': 1394.1744355650976
    },
    62.1: {
        'target_loss_percent': 62.1,
        'actual_margin_needed': 25968.190415,
        'margin_ratio': 1399.436975140883
    },
    62.2: {
        'target_loss_percent': 62.2,
        'actual_margin_needed': 26066.482075,
        'margin_ratio': 1404.7339550672364
    },
    62.300000000000004: {
        'target_loss_percent': 62.300000000000004,
        'actual_margin_needed': 26165.030005,
        'margin_ratio': 1410.0447454943558
    },
    62.400000000000006: {
        'target_loss_percent': 62.400000000000006,
        'actual_margin_needed': 26263.471505,
        'margin_ratio': 1415.3498003628981
    },
    62.5: {
        'target_loss_percent': 62.5,
        'actual_margin_needed': 26362.548837,
        'margin_ratio': 1420.689120491998
    },
    62.6: {
        'target_loss_percent': 62.6,
        'actual_margin_needed': 26461.885179,
        'margin_ratio': 1426.042398781645
    },
    62.7: {
        'target_loss_percent': 62.7,
        'actual_margin_needed': 26561.120472,
        'margin_ratio': 1431.3902314971244
    },
    62.800000000000004: {
        'target_loss_percent': 62.800000000000004,
        'actual_margin_needed': 26660.990765,
        'margin_ratio': 1436.7722846363229
    },
    62.900000000000006: {
        'target_loss_percent': 62.900000000000006,
        'actual_margin_needed': 26761.122786,
        'margin_ratio': 1442.1684424102602
    },
    63.0: {
        'target_loss_percent': 63.0,
        'actual_margin_needed': 26861.156704,
        'margin_ratio': 1447.5593133712398
    },
    63.1: {
        'target_loss_percent': 63.1,
        'actual_margin_needed': 26961.827189,
        'margin_ratio': 1452.9844892022436
    },
    63.2: {
        'target_loss_percent': 63.2,
        'actual_margin_needed': 27062.762098,
        'margin_ratio': 1458.4239149565883
    },
    63.300000000000004: {
        'target_loss_percent': 63.300000000000004,
        'actual_margin_needed': 27163.599437,
        'margin_ratio': 1463.8580826215755
    },
    63.400000000000006: {
        'target_loss_percent': 63.400000000000006,
        'actual_margin_needed': 27265.077286,
        'margin_ratio': 1469.3267676465562
    },
    63.5: {
        'target_loss_percent': 63.5,
        'actual_margin_needed': 27366.822236,
        'margin_ratio': 1474.809846859562
    },
    63.6: {
        'target_loss_percent': 63.6,
        'actual_margin_needed': 27468.835282,
        'margin_ratio': 1480.3073738815713
    },
    63.7: {
        'target_loss_percent': 63.7,
        'actual_margin_needed': 27570.760081,
        'margin_ratio': 1485.8001452347116
    },
    63.800000000000004: {
        'target_loss_percent': 63.800000000000004,
        'actual_margin_needed': 27673.322165,
        'margin_ratio': 1491.3272601512058
    },
    63.900000000000006: {
        'target_loss_percent': 63.900000000000006,
        'actual_margin_needed': 27776.154993,
        'margin_ratio': 1496.868965578565
    },
    64.0: {
        'target_loss_percent': 64.0,
        'actual_margin_needed': 27879.259551,
        'margin_ratio': 1502.4253146527542
    },
    64.1: {
        'target_loss_percent': 64.1,
        'actual_margin_needed': 27982.280703,
        'margin_ratio': 1507.9771689416511
    },
    64.2: {
        'target_loss_percent': 64.2,
        'actual_margin_needed': 28085.940329,
        'margin_ratio': 1513.5634308696244
    },
    64.30000000000001: {
        'target_loss_percent': 64.30000000000001,
        'actual_margin_needed': 28189.874301,
        'margin_ratio': 1519.1644774217955
    },
    64.4: {
        'target_loss_percent': 64.4,
        'actual_margin_needed': 28294.083597,
        'margin_ratio': 1524.780361303006
    },
    64.5: {
        'target_loss_percent': 64.5,
        'actual_margin_needed': 28398.569189,
        'margin_ratio': 1530.4111348947547
    },
    64.6: {
        'target_loss_percent': 64.6,
        'actual_margin_needed': 28502.980227,
        'margin_ratio': 1536.0378907393065
    },
    64.7: {
        'target_loss_percent': 64.7,
        'actual_margin_needed': 28608.028467,
        'margin_ratio': 1541.6989856743066
    },
    64.80000000000001: {
        'target_loss_percent': 64.80000000000001,
        'actual_margin_needed': 28713.355583,
        'margin_ratio': 1547.375109357157
    },
    64.9: {
        'target_loss_percent': 64.9,
        'actual_margin_needed': 28818.962538,
        'margin_ratio': 1553.0663136843432
    },
    65.0: {
        'target_loss_percent': 65.0,
        'actual_margin_needed': 28924.850292,
        'margin_ratio': 1558.7726503906786
    },
    65.10000000000001: {
        'target_loss_percent': 65.10000000000001,
        'actual_margin_needed': 29031.019801,
        'margin_ratio': 1564.4941709954153
    },
    65.2: {
        'target_loss_percent': 65.2,
        'actual_margin_needed': 29137.472019,
        'margin_ratio': 1570.230926910025
    },
    65.3: {
        'target_loss_percent': 65.3,
        'actual_margin_needed': 29243.864361,
        'margin_ratio': 1575.9644560811846
    },
    65.4: {
        'target_loss_percent': 65.4,
        'actual_margin_needed': 29350.889873,
        'margin_ratio': 1581.7321070565745
    },
    65.5: {
        'target_loss_percent': 65.5,
        'actual_margin_needed': 29458.200622,
        'margin_ratio': 1587.515129576847
    },
    65.60000000000001: {
        'target_loss_percent': 65.60000000000001,
        'actual_margin_needed': 29565.797554,
        'margin_ratio': 1593.3135746223493
    },
    65.7: {
        'target_loss_percent': 65.7,
        'actual_margin_needed': 29673.681608,
        'margin_ratio': 1599.1274927961965
    },
    65.8: {
        'target_loss_percent': 65.8,
        'actual_margin_needed': 29781.853722,
        'margin_ratio': 1604.956934647613
    },
    65.9: {
        'target_loss_percent': 65.9,
        'actual_margin_needed': 29890.31483,
        'margin_ratio': 1610.8019505102613
    },
    66.0: {
        'target_loss_percent': 66.0,
        'actual_margin_needed': 29999.065866,
        'margin_ratio': 1616.6625907178043
    },
    66.10000000000001: {
        'target_loss_percent': 66.10000000000001,
        'actual_margin_needed': 30108.107758,
        'margin_ratio': 1622.5389052805615
    },
    66.2: {
        'target_loss_percent': 66.2,
        'actual_margin_needed': 30217.441433,
        'margin_ratio': 1628.4309441549628
    },
    66.3: {
        'target_loss_percent': 66.3,
        'actual_margin_needed': 30327.067814,
        'margin_ratio': 1634.3387570818763
    },
    66.4: {
        'target_loss_percent': 66.4,
        'actual_margin_needed': 30436.987822,
        'margin_ratio': 1640.2623936943883
    },
    66.5: {
        'target_loss_percent': 66.5,
        'actual_margin_needed': 30547.202377,
        'margin_ratio': 1646.201903571696
    },
    66.60000000000001: {
        'target_loss_percent': 66.60000000000001,
        'actual_margin_needed': 30657.712392,
        'margin_ratio': 1652.1573359157628
    },
    66.7: {
        'target_loss_percent': 66.7,
        'actual_margin_needed': 30768.518781,
        'margin_ratio': 1658.1287399824423
    },
    66.8: {
        'target_loss_percent': 66.8,
        'actual_margin_needed': 30879.622454,
        'margin_ratio': 1664.1161648120274
    },
    66.9: {
        'target_loss_percent': 66.9,
        'actual_margin_needed': 30991.024318,
        'margin_ratio': 1670.1196592831386
    },
    67.0: {
        'target_loss_percent': 67.0,
        'actual_margin_needed': 31102.725277,
        'margin_ratio': 1676.1392721127258
    },
    67.10000000000001: {
        'target_loss_percent': 67.10000000000001,
        'actual_margin_needed': 31214.726233,
        'margin_ratio': 1682.1750519099576
    },
    67.2: {
        'target_loss_percent': 67.2,
        'actual_margin_needed': 31327.028087,
        'margin_ratio': 1688.2270472301125
    },
    67.3: {
        'target_loss_percent': 67.3,
        'actual_margin_needed': 31439.631732,
        'margin_ratio': 1694.2953061973456
    },
    67.4: {
        'target_loss_percent': 67.4,
        'actual_margin_needed': 31552.538065,
        'margin_ratio': 1700.3798771513732
    },
    67.5: {
        'target_loss_percent': 67.5,
        'actual_margin_needed': 31665.747975,
        'margin_ratio': 1706.4808080007888
    },
    67.60000000000001: {
        'target_loss_percent': 67.60000000000001,
        'actual_margin_needed': 31779.262351,
        'margin_ratio': 1712.5981466541855
    },
    67.7: {
        'target_loss_percent': 67.7,
        'actual_margin_needed': 31893.082079,
        'margin_ratio': 1718.7319408584851
    },
    67.8: {
        'target_loss_percent': 67.8,
        'actual_margin_needed': 32007.208042,
        'margin_ratio': 1724.8822381989385
    },
    67.9: {
        'target_loss_percent': 67.9,
        'actual_margin_needed': 32121.64112,
        'margin_ratio': 1731.049086099125
    },
    68.0: {
        'target_loss_percent': 68.0,
        'actual_margin_needed': 32236.38219,
        'margin_ratio': 1737.232531820952
    },
    68.10000000000001: {
        'target_loss_percent': 68.10000000000001,
        'actual_margin_needed': 32351.432129,
        'margin_ratio': 1743.4326226263286
    },
    68.2: {
        'target_loss_percent': 68.2,
        'actual_margin_needed': 32467.114017,
        'margin_ratio': 1749.6667694357186
    },
    68.3: {
        'target_loss_percent': 68.3,
        'actual_margin_needed': 32582.788037,
        'margin_ratio': 1755.900492235197
    },
    68.4: {
        'target_loss_percent': 68.4,
        'actual_margin_needed': 32698.773823,
        'margin_ratio': 1762.1510162388033
    },
    68.5: {
        'target_loss_percent': 68.5,
        'actual_margin_needed': 32815.072239,
        'margin_ratio': 1768.4183880078701
    },
    68.60000000000001: {
        'target_loss_percent': 68.60000000000001,
        'actual_margin_needed': 32931.684147,
        'margin_ratio': 1774.7026539959484
    },
    68.7: {
        'target_loss_percent': 68.7,
        'actual_margin_needed': 33048.610405,
        'margin_ratio': 1781.0038604410283
    },
    68.8: {
        'target_loss_percent': 68.8,
        'actual_margin_needed': 33166.169217,
        'margin_ratio': 1787.339155493833
    },
    68.9: {
        'target_loss_percent': 68.9,
        'actual_margin_needed': 33283.732283,
        'margin_ratio': 1793.674679796531
    },
    69.0: {
        'target_loss_percent': 69.0,
        'actual_margin_needed': 33401.612544,
        'margin_ratio': 1800.0272978745074
    },
    69.10000000000001: {
        'target_loss_percent': 69.10000000000001,
        'actual_margin_needed': 33519.810845,
        'margin_ratio': 1806.3970552651758
    },
    69.2: {
        'target_loss_percent': 69.2,
        'actual_margin_needed': 33638.638105,
        'margin_ratio': 1812.800707527469
    },
    69.3: {
        'target_loss_percent': 69.3,
        'actual_margin_needed': 33757.481785,
        'margin_ratio': 1819.2052446706407
    },
    69.4: {
        'target_loss_percent': 69.4,
        'actual_margin_needed': 33876.646313,
        'margin_ratio': 1825.627072450834
    },
    69.5: {
        'target_loss_percent': 69.5,
        'actual_margin_needed': 33996.132526,
        'margin_ratio': 1832.0662359743417
    },
    69.60000000000001: {
        'target_loss_percent': 69.60000000000001,
        'actual_margin_needed': 34116.249317,
        'margin_ratio': 1838.5393816180817
    },
    69.7: {
        'target_loss_percent': 69.7,
        'actual_margin_needed': 34236.389368,
        'margin_ratio': 1845.0137807532478
    },
    69.8: {
        'target_loss_percent': 69.8,
        'actual_margin_needed': 34356.853874,
        'margin_ratio': 1851.5056649082217
    },
    69.9: {
        'target_loss_percent': 69.9,
        'actual_margin_needed': 34477.946018,
        'margin_ratio': 1858.0313727455612
    },
    70.0: {
        'target_loss_percent': 70.0,
        'actual_margin_needed': 34599.070794,
        'margin_ratio': 1864.5588391354463
    },
    70.10000000000001: {
        'target_loss_percent': 70.10000000000001,
        'actual_margin_needed': 34720.522769,
        'margin_ratio': 1871.1039384204823
    },
    70.2: {
        'target_loss_percent': 70.2,
        'actual_margin_needed': 34842.602458,
        'margin_ratio': 1877.682865483556
    },
    70.3: {
        'target_loss_percent': 70.3,
        'actual_margin_needed': 34964.721072,
        'margin_ratio': 1884.2638902316587
    },
    70.4: {
        'target_loss_percent': 70.4,
        'actual_margin_needed': 35087.1696,
        'margin_ratio': 1890.862694187432
    },
    70.5: {
        'target_loss_percent': 70.5,
        'actual_margin_needed': 35210.248904,
        'margin_ratio': 1897.4954909337432
    },
    70.60000000000001: {
        'target_loss_percent': 70.60000000000001,
        'actual_margin_needed': 35333.370376,
        'margin_ratio': 1904.130560131751
    },
    70.7: {
        'target_loss_percent': 70.7,
        'actual_margin_needed': 35456.824451,
        'margin_ratio': 1910.7835534487988
    },
    70.8: {
        'target_loss_percent': 70.8,
        'actual_margin_needed': 35580.915319,
        'margin_ratio': 1917.4708638151083
    },
    70.9: {
        'target_loss_percent': 70.9,
        'actual_margin_needed': 35705.048581,
        'margin_ratio': 1924.1604588123519
    },
    71.0: {
        'target_loss_percent': 71.0,
        'actual_margin_needed': 35829.814591,
        'margin_ratio': 1930.8841528720636
    },
    71.10000000000001: {
        'target_loss_percent': 71.10000000000001,
        'actual_margin_needed': 35954.631372,
        'margin_ratio': 1937.6105830028503
    },
    71.2: {
        'target_loss_percent': 71.2,
        'actual_margin_needed': 36080.078281,
        'margin_ratio': 1944.3709710031758
    },
    71.3: {
        'target_loss_percent': 71.3,
        'actual_margin_needed': 36205.582883,
        'margin_ratio': 1951.13446810414
    },
    71.4: {
        'target_loss_percent': 71.4,
        'actual_margin_needed': 36331.716406,
        'margin_ratio': 1957.9318580288925
    },
    71.5: {
        'target_loss_percent': 71.5,
        'actual_margin_needed': 36457.913103,
        'margin_ratio': 1964.7326524277366
    },
    71.60000000000001: {
        'target_loss_percent': 71.60000000000001,
        'actual_margin_needed': 36584.738918,
        'margin_ratio': 1971.5673502667846
    },
    71.7: {
        'target_loss_percent': 71.7,
        'actual_margin_needed': 36711.631951,
        'margin_ratio': 1978.4056705128266
    },
    71.8: {
        'target_loss_percent': 71.8,
        'actual_margin_needed': 36839.155695,
        'margin_ratio': 1985.277980046529
    },
    71.9: {
        'target_loss_percent': 71.9,
        'actual_margin_needed': 36966.749277,
        'margin_ratio': 1992.1540531801552
    },
    72.0: {
        'target_loss_percent': 72.0,
        'actual_margin_needed': 37094.976547,
        'margin_ratio': 1999.0642760332544
    },
    72.10000000000001: {
        'target_loss_percent': 72.10000000000001,
        'actual_margin_needed': 37223.274863,
        'margin_ratio': 2005.9783275859186
    },
    72.2: {
        'target_loss_percent': 72.2,
        'actual_margin_needed': 37352.211218,
        'margin_ratio': 2012.9267633353215
    },
    72.3: {
        'target_loss_percent': 72.3,
        'actual_margin_needed': 37481.498103,
        'margin_ratio': 2019.8940892975218
    },
    72.4: {
        'target_loss_percent': 72.4,
        'actual_margin_needed': 37610.869381,
        'margin_ratio': 2026.8659632348697
    },
    72.5: {
        'target_loss_percent': 72.5,
        'actual_margin_needed': 37740.874008,
        'margin_ratio': 2033.871968622835
    },
    72.60000000000001: {
        'target_loss_percent': 72.60000000000001,
        'actual_margin_needed': 37870.960644,
        'margin_ratio': 2040.8823935111607
    },
    72.7: {
        'target_loss_percent': 72.7,
        'actual_margin_needed': 38001.688471,
        'margin_ratio': 2047.927372458864
    },
    72.8: {
        'target_loss_percent': 72.8,
        'actual_margin_needed': 38132.77485,
        'margin_ratio': 2054.991673928402
    },
    72.9: {
        'target_loss_percent': 72.9,
        'actual_margin_needed': 38263.950994,
        'margin_ratio': 2062.060812872484
    },
    73.0: {
        'target_loss_percent': 73.0,
        'actual_margin_needed': 38395.769077,
        'margin_ratio': 2069.1645461859853
    },
    73.10000000000001: {
        'target_loss_percent': 73.10000000000001,
        'actual_margin_needed': 38527.950542,
        'margin_ratio': 2076.287862312104
    },
    73.2: {
        'target_loss_percent': 73.2,
        'actual_margin_needed': 38660.497134,
        'margin_ratio': 2083.430855289642
    },
    73.3: {
        'target_loss_percent': 73.3,
        'actual_margin_needed': 38793.147738,
        'margin_ratio': 2090.5794535186947
    },
    73.4: {
        'target_loss_percent': 73.4,
        'actual_margin_needed': 38926.437478,
        'margin_ratio': 2097.762495062289
    },
    73.5: {
        'target_loss_percent': 73.5,
        'actual_margin_needed': 39060.097104,
        'margin_ratio': 2104.965469921864
    },
    73.60000000000001: {
        'target_loss_percent': 73.60000000000001,
        'actual_margin_needed': 39194.128336,
        'margin_ratio': 2112.1884707889612
    },
    73.7: {
        'target_loss_percent': 73.7,
        'actual_margin_needed': 39328.271654,
        'margin_ratio': 2119.417512018913
    },
    73.8: {
        'target_loss_percent': 73.8,
        'actual_margin_needed': 39463.057306,
        'margin_ratio': 2126.6811689050055
    },
    73.9: {
        'target_loss_percent': 73.9,
        'actual_margin_needed': 39598.219252,
        'margin_ratio': 2133.9651044369607
    },
    74.0: {
        'target_loss_percent': 74.0,
        'actual_margin_needed': 39733.759183,
        'margin_ratio': 2141.269409743498
    },
    74.10000000000001: {
        'target_loss_percent': 74.10000000000001,
        'actual_margin_needed': 39869.678786,
        'margin_ratio': 2148.594175737773
    },
    74.2: {
        'target_loss_percent': 74.2,
        'actual_margin_needed': 40005.979739,
        'margin_ratio': 2155.9394928479314
    },
    74.3: {
        'target_loss_percent': 74.3,
        'actual_margin_needed': 40142.406721,
        'margin_ratio': 2163.2916017152243
    },
    74.4: {
        'target_loss_percent': 74.4,
        'actual_margin_needed': 40279.479056,
        'margin_ratio': 2170.678488933869
    },
    74.5: {
        'target_loss_percent': 74.5,
        'actual_margin_needed': 40416.937323,
        'margin_ratio': 2178.0861741943513
    },
    74.60000000000001: {
        'target_loss_percent': 74.60000000000001,
        'actual_margin_needed': 40554.783176,
        'margin_ratio': 2185.5147466314434
    },
    74.7: {
        'target_loss_percent': 74.7,
        'actual_margin_needed': 40693.018263,
        'margin_ratio': 2192.9642950565767
    },
    74.8: {
        'target_loss_percent': 74.8,
        'actual_margin_needed': 40831.644223,
        'margin_ratio': 2200.434907796167
    },
    74.9: {
        'target_loss_percent': 74.9,
        'actual_margin_needed': 40970.662692,
        'margin_ratio': 2207.9266730149593
    },
    75.0: {
        'target_loss_percent': 75.0,
        'actual_margin_needed': 41110.075295,
        'margin_ratio': 2215.439678284905
    },
    75.10000000000001: {
        'target_loss_percent': 75.10000000000001,
        'actual_margin_needed': 41249.883656,
        'margin_ratio': 2222.9740111240626
    },
    75.2: {
        'target_loss_percent': 75.2,
        'actual_margin_needed': 41390.089387,
        'margin_ratio': 2230.5297584038085
    },
    75.3: {
        'target_loss_percent': 75.3,
        'actual_margin_needed': 41530.694097,
        'margin_ratio': 2238.1070068338454
    },
    75.4: {
        'target_loss_percent': 75.4,
        'actual_margin_needed': 41671.699388,
        'margin_ratio': 2245.705842746644
    },
    75.5: {
        'target_loss_percent': 75.5,
        'actual_margin_needed': 41813.106855,
        'margin_ratio': 2253.3263520974424
    },
    75.60000000000001: {
        'target_loss_percent': 75.60000000000001,
        'actual_margin_needed': 41954.918087,
        'margin_ratio': 2260.9686205181347
    },
    75.7: {
        'target_loss_percent': 75.7,
        'actual_margin_needed': 42097.134667,
        'margin_ratio': 2268.6327333172735
    },
    75.8: {
        'target_loss_percent': 75.8,
        'actual_margin_needed': 42240.002812,
        'margin_ratio': 2276.331959235122
    },
    75.9: {
        'target_loss_percent': 75.9,
        'actual_margin_needed': 42383.037178,
        'margin_ratio': 2284.0401428743107
    },
    76.0: {
        'target_loss_percent': 76.0,
        'actual_margin_needed': 42526.48199,
        'margin_ratio': 2291.770445625363
    },
    76.10000000000001: {
        'target_loss_percent': 76.10000000000001,
        'actual_margin_needed': 42670.338806,
        'margin_ratio': 2299.5229514495713
    },
    76.2: {
        'target_loss_percent': 76.2,
        'actual_margin_needed': 42814.609175,
        'margin_ratio': 2307.2977438232133
    },
    76.3: {
        'target_loss_percent': 76.3,
        'actual_margin_needed': 42959.29464,
        'margin_ratio': 2315.0949058992237
    },
    76.4: {
        'target_loss_percent': 76.4,
        'actual_margin_needed': 43104.637542,
        'margin_ratio': 2322.9274975385542
    },
    76.5: {
        'target_loss_percent': 76.5,
        'actual_margin_needed': 43250.162585,
        'margin_ratio': 2330.7699048348877
    },
    76.60000000000001: {
        'target_loss_percent': 76.60000000000001,
        'actual_margin_needed': 43396.107699,
        'margin_ratio': 2338.6349499384837
    },
    76.7: {
        'target_loss_percent': 76.7,
        'actual_margin_needed': 43542.4744,
        'margin_ratio': 2346.522714547237
    },
    76.8: {
        'target_loss_percent': 76.8,
        'actual_margin_needed': 43689.502936,
        'margin_ratio': 2354.4461457294256
    },
    76.9: {
        'target_loss_percent': 76.9,
        'actual_margin_needed': 43836.723726,
        'margin_ratio': 2362.3799375625463
    },
    77.0: {
        'target_loss_percent': 77.0,
        'actual_margin_needed': 43984.370994,
        'margin_ratio': 2370.3367124789215
    },
    77.10000000000001: {
        'target_loss_percent': 77.10000000000001,
        'actual_margin_needed': 44132.682084,
        'margin_ratio': 2378.3292610490203
    },
    77.2: {
        'target_loss_percent': 77.2,
        'actual_margin_needed': 44281.194372,
        'margin_ratio': 2386.3326522660655
    },
    77.3: {
        'target_loss_percent': 77.3,
        'actual_margin_needed': 44430.366426,
        'margin_ratio': 2394.3715985572453
    },
    77.4: {
        'target_loss_percent': 77.4,
        'actual_margin_needed': 44579.751219,
        'margin_ratio': 2402.4220094448365
    },
    77.5: {
        'target_loss_percent': 77.5,
        'actual_margin_needed': 44729.570621,
        'margin_ratio': 2410.495841598778
    },
    77.60000000000001: {
        'target_loss_percent': 77.60000000000001,
        'actual_margin_needed': 44880.058266,
        'margin_ratio': 2418.6056856560376
    },
    77.7: {
        'target_loss_percent': 77.7,
        'actual_margin_needed': 45030.760866,
        'margin_ratio': 2426.7271137309044
    },
    77.80000000000001: {
        'target_loss_percent': 77.80000000000001,
        'actual_margin_needed': 45182.132094,
        'margin_ratio': 2434.8845744569057
    },
    77.9: {
        'target_loss_percent': 77.9,
        'actual_margin_needed': 45333.725214,
        'margin_ratio': 2443.053993038434
    },
    78.0: {
        'target_loss_percent': 78.0,
        'actual_margin_needed': 45485.989138,
        'margin_ratio': 2451.2595615366745
    },
    78.10000000000001: {
        'target_loss_percent': 78.10000000000001,
        'actual_margin_needed': 45638.480034,
        'margin_ratio': 2459.4773616538323
    },
    78.2: {
        'target_loss_percent': 78.2,
        'actual_margin_needed': 45791.645682,
        'margin_ratio': 2467.7315244471247
    },
    78.30000000000001: {
        'target_loss_percent': 78.30000000000001,
        'actual_margin_needed': 45945.260604,
        'margin_ratio': 2476.0098988099376
    },
    78.4: {
        'target_loss_percent': 78.4,
        'actual_margin_needed': 46099.117866,
        'margin_ratio': 2484.301332979812
    },
    78.5: {
        'target_loss_percent': 78.5,
        'actual_margin_needed': 46253.648398,
        'margin_ratio': 2492.6290499602014
    },
    78.60000000000001: {
        'target_loss_percent': 78.60000000000001,
        'actual_margin_needed': 46408.635864,
        'margin_ratio': 2500.9813913108146
    },
    78.7: {
        'target_loss_percent': 78.7,
        'actual_margin_needed': 46563.876652,
        'margin_ratio': 2509.3473842932026
    },
    78.80000000000001: {
        'target_loss_percent': 78.80000000000001,
        'actual_margin_needed': 46719.79342,
        'margin_ratio': 2517.749806129174
    },
    78.9: {
        'target_loss_percent': 78.9,
        'actual_margin_needed': 46876.174677,
        'margin_ratio': 2526.1772594775753
    },
    79.0: {
        'target_loss_percent': 79.0,
        'actual_margin_needed': 47032.81591,
        'margin_ratio': 2534.61872304468
    },
    79.10000000000001: {
        'target_loss_percent': 79.10000000000001,
        'actual_margin_needed': 47190.13997,
        'margin_ratio': 2543.096984453149
    },
    79.2: {
        'target_loss_percent': 79.2,
        'actual_margin_needed': 47347.935968,
        'margin_ratio': 2551.6006788038694
    },
    79.30000000000001: {
        'target_loss_percent': 79.30000000000001,
        'actual_margin_needed': 47506.206537,
        'margin_ratio': 2560.1299479903446
    },
    79.4: {
        'target_loss_percent': 79.4,
        'actual_margin_needed': 47664.9543,
        'margin_ratio': 2568.684933367176
    },
    79.5: {
        'target_loss_percent': 79.5,
        'actual_margin_needed': 47823.977844,
        'margin_ratio': 2577.254780700974
    },
    79.60000000000001: {
        'target_loss_percent': 79.60000000000001,
        'actual_margin_needed': 47983.691122,
        'margin_ratio': 2585.861798097344
    },
    79.7: {
        'target_loss_percent': 79.7,
        'actual_margin_needed': 48143.888865,
        'margin_ratio': 2594.4949235213944
    },
    79.80000000000001: {
        'target_loss_percent': 79.80000000000001,
        'actual_margin_needed': 48304.573646,
        'margin_ratio': 2603.154295633204
    },
    79.9: {
        'target_loss_percent': 79.9,
        'actual_margin_needed': 48465.748026,
        'margin_ratio': 2611.840052446167
    },
    80.0: {
        'target_loss_percent': 80.0,
        'actual_margin_needed': 48627.41455,
        'margin_ratio': 2620.5523311114307
    },
    80.10000000000001: {
        'target_loss_percent': 80.10000000000001,
        'actual_margin_needed': 48789.575753,
        'margin_ratio': 2629.291268241237
    },
    80.2: {
        'target_loss_percent': 80.2,
        'actual_margin_needed': 48952.234157,
        'margin_ratio': 2638.0569997472535
    },
    80.30000000000001: {
        'target_loss_percent': 80.30000000000001,
        'actual_margin_needed': 49115.39227,
        'margin_ratio': 2646.8496607866814
    },
    80.4: {
        'target_loss_percent': 80.4,
        'actual_margin_needed': 49279.05259,
        'margin_ratio': 2655.6693859778175
    },
    80.5: {
        'target_loss_percent': 80.5,
        'actual_margin_needed': 49443.217599,
        'margin_ratio': 2664.5163090767114
    },
    80.60000000000001: {
        'target_loss_percent': 80.60000000000001,
        'actual_margin_needed': 49608.08528,
        'margin_ratio': 2673.401099472574
    },
    80.7: {
        'target_loss_percent': 80.7,
        'actual_margin_needed': 49773.26929,
        'margin_ratio': 2682.3029369746014
    },
    80.80000000000001: {
        'target_loss_percent': 80.80000000000001,
        'actual_margin_needed': 49938.965856,
        'margin_ratio': 2691.2323963404083
    },
    80.9: {
        'target_loss_percent': 80.9,
        'actual_margin_needed': 50105.17741,
        'margin_ratio': 2700.1896086315223
    },
    81.0: {
        'target_loss_percent': 81.0,
        'actual_margin_needed': 50271.906373,
        'margin_ratio': 2709.174704316677
    },
    81.10000000000001: {
        'target_loss_percent': 81.10000000000001,
        'actual_margin_needed': 50439.347497,
        'margin_ratio': 2718.1981786651018
    },
    81.2: {
        'target_loss_percent': 81.2,
        'actual_margin_needed': 50607.123217,
        'margin_ratio': 2727.2396845361945
    },
    81.30000000000001: {
        'target_loss_percent': 81.30000000000001,
        'actual_margin_needed': 50775.42401,
        'margin_ratio': 2736.309486817592
    },
    81.4: {
        'target_loss_percent': 81.4,
        'actual_margin_needed': 50944.441393,
        'margin_ratio': 2745.417906442978
    },
    81.5: {
        'target_loss_percent': 81.5,
        'actual_margin_needed': 51113.80567,
        'margin_ratio': 2754.545020335554
    },
    81.60000000000001: {
        'target_loss_percent': 81.60000000000001,
        'actual_margin_needed': 51283.702561,
        'margin_ratio': 2763.7008370261747
    },
    81.7: {
        'target_loss_percent': 81.7,
        'actual_margin_needed': 51454.325165,
        'margin_ratio': 2772.895762703187
    },
    81.80000000000001: {
        'target_loss_percent': 81.80000000000001,
        'actual_margin_needed': 51625.302001,
        'margin_ratio': 2782.1097780953714
    },
    81.9: {
        'target_loss_percent': 81.9,
        'actual_margin_needed': 51797.007255,
        'margin_ratio': 2791.3630482475637
    },
    82.0: {
        'target_loss_percent': 82.0,
        'actual_margin_needed': 51969.0752,
        'margin_ratio': 2800.6358639740847
    },
    82.10000000000001: {
        'target_loss_percent': 82.10000000000001,
        'actual_margin_needed': 52141.876428,
        'margin_ratio': 2809.94819663756
    },
    82.2: {
        'target_loss_percent': 82.2,
        'actual_margin_needed': 52315.226479,
        'margin_ratio': 2819.2901056090773
    },
    82.30000000000001: {
        'target_loss_percent': 82.30000000000001,
        'actual_margin_needed': 52488.956906,
        'margin_ratio': 2828.652513207197
    },
    82.4: {
        'target_loss_percent': 82.4,
        'actual_margin_needed': 52663.423237,
        'margin_ratio': 2838.0545789890907
    },
    82.5: {
        'target_loss_percent': 82.5,
        'actual_margin_needed': 52838.449372,
        'margin_ratio': 2847.4868128498497
    },
    82.60000000000001: {
        'target_loss_percent': 82.60000000000001,
        'actual_margin_needed': 53013.868211,
        'margin_ratio': 2856.940209698445
    },
    82.7: {
        'target_loss_percent': 82.7,
        'actual_margin_needed': 53190.030589,
        'margin_ratio': 2866.4336761842555
    },
    82.80000000000001: {
        'target_loss_percent': 82.80000000000001,
        'actual_margin_needed': 53366.763568,
        'margin_ratio': 2875.9578926039153
    },
    82.9: {
        'target_loss_percent': 82.9,
        'actual_margin_needed': 53544.070915,
        'margin_ratio': 2885.5130619626784
    },
    83.0: {
        'target_loss_percent': 83.0,
        'actual_margin_needed': 53721.956375,
        'margin_ratio': 2895.099386080209
    },
    83.10000000000001: {
        'target_loss_percent': 83.10000000000001,
        'actual_margin_needed': 53900.255224,
        'margin_ratio': 2904.7079879091425
    },
    83.2: {
        'target_loss_percent': 83.2,
        'actual_margin_needed': 54079.310207,
        'margin_ratio': 2914.3573381252704
    },
    83.30000000000001: {
        'target_loss_percent': 83.30000000000001,
        'actual_margin_needed': 54258.95379,
        'margin_ratio': 2924.038408249116
    },
    83.4: {
        'target_loss_percent': 83.4,
        'actual_margin_needed': 54439.189633,
        'margin_ratio': 2933.7513955196573
    },
    83.5: {
        'target_loss_percent': 83.5,
        'actual_margin_needed': 54620.021373,
        'margin_ratio': 2943.496495936392
    },
    83.60000000000001: {
        'target_loss_percent': 83.60000000000001,
        'actual_margin_needed': 54801.452626,
        'margin_ratio': 2953.2739043671186
    },
    83.7: {
        'target_loss_percent': 83.7,
        'actual_margin_needed': 54983.651598,
        'margin_ratio': 2963.092685505683
    },
    83.80000000000001: {
        'target_loss_percent': 83.80000000000001,
        'actual_margin_needed': 55166.294095,
        'margin_ratio': 2972.935368397681
    },
    83.9: {
        'target_loss_percent': 83.9,
        'actual_margin_needed': 55349.547412,
        'margin_ratio': 2982.8109686427747
    },
    84.0: {
        'target_loss_percent': 84.0,
        'actual_margin_needed': 55533.415074,
        'margin_ratio': 2992.719676204734
    },
    84.10000000000001: {
        'target_loss_percent': 84.10000000000001,
        'actual_margin_needed': 55717.900588,
        'margin_ratio': 3002.6616800773
    },
    84.2: {
        'target_loss_percent': 84.2,
        'actual_margin_needed': 55903.169229,
        'margin_ratio': 3012.6458870014662
    },
    84.30000000000001: {
        'target_loss_percent': 84.30000000000001,
        'actual_margin_needed': 56088.905345,
        'margin_ratio': 3022.6552863548886
    },
    84.4: {
        'target_loss_percent': 84.4,
        'actual_margin_needed': 56275.270274,
        'margin_ratio': 3032.6985727119336
    },
    84.5: {
        'target_loss_percent': 84.5,
        'actual_margin_needed': 56462.428667,
        'margin_ratio': 3042.784619185963
    },
    84.60000000000001: {
        'target_loss_percent': 84.60000000000001,
        'actual_margin_needed': 56650.067724,
        'margin_ratio': 3052.896568850145
    },
    84.7: {
        'target_loss_percent': 84.7,
        'actual_margin_needed': 56838.505203,
        'margin_ratio': 3063.0515458200694
    },
    84.80000000000001: {
        'target_loss_percent': 84.80000000000001,
        'actual_margin_needed': 57027.434229,
        'margin_ratio': 3073.2330124697064
    },
    84.9: {
        'target_loss_percent': 84.9,
        'actual_margin_needed': 57217.169156,
        'margin_ratio': 3083.4579094716205
    },
    85.0: {
        'target_loss_percent': 85.0,
        'actual_margin_needed': 57407.556385,
        'margin_ratio': 3093.717959309491
    },
    85.10000000000001: {
        'target_loss_percent': 85.10000000000001,
        'actual_margin_needed': 57598.454287,
        'margin_ratio': 3104.0055295354587
    },
    85.2: {
        'target_loss_percent': 85.2,
        'actual_margin_needed': 57790.16675,
        'margin_ratio': 3114.336996804836
    },
    85.30000000000001: {
        'target_loss_percent': 85.30000000000001,
        'actual_margin_needed': 57982.546326,
        'margin_ratio': 3124.704415081345
    },
    85.4: {
        'target_loss_percent': 85.4,
        'actual_margin_needed': 58175.598132,
        'margin_ratio': 3135.1080601223202
    },
    85.5: {
        'target_loss_percent': 85.5,
        'actual_margin_needed': 58369.327246,
        'margin_ratio': 3145.548205583372
    },
    85.60000000000001: {
        'target_loss_percent': 85.60000000000001,
        'actual_margin_needed': 58563.595108,
        'margin_ratio': 3156.0173844063725
    },
    85.7: {
        'target_loss_percent': 85.7,
        'actual_margin_needed': 58758.695435,
        'margin_ratio': 3166.53142512706
    },
    85.80000000000001: {
        'target_loss_percent': 85.80000000000001,
        'actual_margin_needed': 58954.487388,
        'margin_ratio': 3177.082737871015
    },
    85.9: {
        'target_loss_percent': 85.9,
        'actual_margin_needed': 59151.117134,
        'margin_ratio': 3187.679199640874
    },
    86.0: {
        'target_loss_percent': 86.0,
        'actual_margin_needed': 59348.307745,
        'margin_ratio': 3198.3058866673455
    },
    86.10000000000001: {
        'target_loss_percent': 86.10000000000001,
        'actual_margin_needed': 59546.205403,
        'margin_ratio': 3208.970676761422
    },
    86.2: {
        'target_loss_percent': 86.2,
        'actual_margin_needed': 59744.814945,
        'margin_ratio': 3219.6738305911185
    },
    86.30000000000001: {
        'target_loss_percent': 86.30000000000001,
        'actual_margin_needed': 59944.141172,
        'margin_ratio': 3230.4156068843945
    },
    86.4: {
        'target_loss_percent': 86.4,
        'actual_margin_needed': 60144.326266,
        'margin_ratio': 3241.203667890518
    },
    86.5: {
        'target_loss_percent': 86.5,
        'actual_margin_needed': 60345.104265,
        'margin_ratio': 3252.0236808026652
    },
    86.60000000000001: {
        'target_loss_percent': 86.60000000000001,
        'actual_margin_needed': 60546.613825,
        'margin_ratio': 3262.8831178524447
    },
    86.7: {
        'target_loss_percent': 86.7,
        'actual_margin_needed': 60748.997954,
        'margin_ratio': 3273.7896858026193
    },
    86.80000000000001: {
        'target_loss_percent': 86.80000000000001,
        'actual_margin_needed': 60951.990814,
        'margin_ratio': 3284.72905852878
    },
    86.9: {
        'target_loss_percent': 86.9,
        'actual_margin_needed': 61155.86796,
        'margin_ratio': 3295.7160858086554
    },
    87.0: {
        'target_loss_percent': 87.0,
        'actual_margin_needed': 61360.497858,
        'margin_ratio': 3306.7436792182866
    },
    87.10000000000001: {
        'target_loss_percent': 87.10000000000001,
        'actual_margin_needed': 61565.759322,
        'margin_ratio': 3317.805307991897
    },
    87.2: {
        'target_loss_percent': 87.2,
        'actual_margin_needed': 61771.918794,
        'margin_ratio': 3328.915330803716
    },
    87.30000000000001: {
        'target_loss_percent': 87.30000000000001,
        'actual_margin_needed': 61978.850292,
        'margin_ratio': 3340.06695842946
    },
    87.4: {
        'target_loss_percent': 87.4,
        'actual_margin_needed': 62186.560417,
        'margin_ratio': 3351.260546599864
    },
    87.5: {
        'target_loss_percent': 87.5,
        'actual_margin_needed': 62395.055718,
        'margin_ratio': 3362.4964482433606
    },
    87.60000000000001: {
        'target_loss_percent': 87.60000000000001,
        'actual_margin_needed': 62604.342689,
        'margin_ratio': 3373.7750133244085
    },
    87.7: {
        'target_loss_percent': 87.7,
        'actual_margin_needed': 62814.427776,
        'margin_ratio': 3385.0965892207278
    },
    87.80000000000001: {
        'target_loss_percent': 87.80000000000001,
        'actual_margin_needed': 63025.31737,
        'margin_ratio': 3396.461520346062
    },
    87.9: {
        'target_loss_percent': 87.9,
        'actual_margin_needed': 63237.017814,
        'margin_ratio': 3407.870148527417
    },
    88.0: {
        'target_loss_percent': 88.0,
        'actual_margin_needed': 63449.657862,
        'margin_ratio': 3419.3294123733504
    },
    88.10000000000001: {
        'target_loss_percent': 88.10000000000001,
        'actual_margin_needed': 63663.002187,
        'margin_ratio': 3430.826630010395
    },
    88.2: {
        'target_loss_percent': 88.2,
        'actual_margin_needed': 63877.294879,
        'margin_ratio': 3442.3749555538666
    },
    88.30000000000001: {
        'target_loss_percent': 88.30000000000001,
        'actual_margin_needed': 64092.311015,
        'margin_ratio': 3453.962267806341
    },
    88.4: {
        'target_loss_percent': 88.4,
        'actual_margin_needed': 64308.287503,
        'margin_ratio': 3465.6013338420585
    },
    88.5: {
        'target_loss_percent': 88.5,
        'actual_margin_needed': 64525.002909,
        'margin_ratio': 3477.2802204873096
    },
    88.60000000000001: {
        'target_loss_percent': 88.60000000000001,
        'actual_margin_needed': 64742.693825,
        'margin_ratio': 3489.011677786957
    },
    88.7: {
        'target_loss_percent': 88.7,
        'actual_margin_needed': 64961.251661,
        'margin_ratio': 3500.7898537790934
    },
    88.80000000000001: {
        'target_loss_percent': 88.80000000000001,
        'actual_margin_needed': 65180.684782,
        'margin_ratio': 3512.615199257174
    },
    88.9: {
        'target_loss_percent': 88.9,
        'actual_margin_needed': 65401.001476,
        'margin_ratio': 3524.4881608650917
    },
    89.0: {
        'target_loss_percent': 89.0,
        'actual_margin_needed': 65622.209959,
        'margin_ratio': 3536.4091813666287
    },
    89.10000000000001: {
        'target_loss_percent': 89.10000000000001,
        'actual_margin_needed': 65844.318372,
        'margin_ratio': 3548.378699483783
    },
    89.2: {
        'target_loss_percent': 89.2,
        'actual_margin_needed': 66067.334784,
        'margin_ratio': 3560.3971500584444
    },
    89.30000000000001: {
        'target_loss_percent': 89.30000000000001,
        'actual_margin_needed': 66291.267189,
        'margin_ratio': 3572.4649638907167
    },
    89.4: {
        'target_loss_percent': 89.4,
        'actual_margin_needed': 66516.231589,
        'margin_ratio': 3584.588392378384
    },
    89.5: {
        'target_loss_percent': 89.5,
        'actual_margin_needed': 66742.023697,
        'margin_ratio': 3596.7564264069574
    },
    89.60000000000001: {
        'target_loss_percent': 89.60000000000001,
        'actual_margin_needed': 66968.862664,
        'margin_ratio': 3608.9808761182935
    },
    89.7: {
        'target_loss_percent': 89.7,
        'actual_margin_needed': 67196.549126,
        'margin_ratio': 3621.2509977004947
    },
    89.80000000000001: {
        'target_loss_percent': 89.80000000000001,
        'actual_margin_needed': 67425.300798,
        'margin_ratio': 3633.5785239087586
    },
    89.9: {
        'target_loss_percent': 89.9,
        'actual_margin_needed': 67655.0216,
        'margin_ratio': 3645.958276950469
    },
    90.0: {
        'target_loss_percent': 90.0,
        'actual_margin_needed': 67885.721795,
        'margin_ratio': 3658.390809903121
    },
    90.10000000000001: {
        'target_loss_percent': 90.10000000000001,
        'actual_margin_needed': 68117.411544,
        'margin_ratio': 3670.876670347383
    },
    90.2: {
        'target_loss_percent': 90.2,
        'actual_margin_needed': 68350.100905,
        'margin_ratio': 3683.416400313211
    },
    90.30000000000001: {
        'target_loss_percent': 90.30000000000001,
        'actual_margin_needed': 68583.799838,
        'margin_ratio': 3696.0105365492986
    },
    90.4: {
        'target_loss_percent': 90.4,
        'actual_margin_needed': 68818.616484,
        'margin_ratio': 3708.66490682075
    },
    90.5: {
        'target_loss_percent': 90.5,
        'actual_margin_needed': 69054.367493,
        'margin_ratio': 3721.369630317032
    },
    90.60000000000001: {
        'target_loss_percent': 90.60000000000001,
        'actual_margin_needed': 69291.254963,
        'margin_ratio': 3734.135598186479
    },
    90.7: {
        'target_loss_percent': 90.7,
        'actual_margin_needed': 69529.100628,
        'margin_ratio': 3746.9532035975094
    },
    90.80000000000001: {
        'target_loss_percent': 90.80000000000001,
        'actual_margin_needed': 69768.10527,
        'margin_ratio': 3759.833266778651
    },
    90.9: {
        'target_loss_percent': 90.9,
        'actual_margin_needed': 70008.184697,
        'margin_ratio': 3772.771250586732
    },
    91.0: {
        'target_loss_percent': 91.0,
        'actual_margin_needed': 70249.351227,
        'margin_ratio': 3785.7678188440827
    },
    91.10000000000001: {
        'target_loss_percent': 91.10000000000001,
        'actual_margin_needed': 70491.617039,
        'margin_ratio': 3798.8236278822624
    },
    91.2: {
        'target_loss_percent': 91.2,
        'actual_margin_needed': 70735.083988,
        'margin_ratio': 3811.944166711126
    },
    91.30000000000001: {
        'target_loss_percent': 91.30000000000001,
        'actual_margin_needed': 70979.587076,
        'margin_ratio': 3825.120543517331
    },
    91.4: {
        'target_loss_percent': 91.4,
        'actual_margin_needed': 71225.313834,
        'margin_ratio': 3838.3628644273035
    },
    91.5: {
        'target_loss_percent': 91.5,
        'actual_margin_needed': 71472.105672,
        'margin_ratio': 3851.6625829575823
    },
    91.60000000000001: {
        'target_loss_percent': 91.60000000000001,
        'actual_margin_needed': 71720.147751,
        'margin_ratio': 3865.029677514271
    },
    91.7: {
        'target_loss_percent': 91.7,
        'actual_margin_needed': 71969.367264,
        'margin_ratio': 3878.4602244967573
    },
    91.80000000000001: {
        'target_loss_percent': 91.80000000000001,
        'actual_margin_needed': 72219.778801,
        'margin_ratio': 3891.955010166429
    },
    91.9: {
        'target_loss_percent': 91.9,
        'actual_margin_needed': 72471.478182,
        'margin_ratio': 3905.5191983043915
    },
    92.0: {
        'target_loss_percent': 92.0,
        'actual_margin_needed': 72724.319205,
        'margin_ratio': 3919.144910021842
    },
    92.10000000000001: {
        'target_loss_percent': 92.10000000000001,
        'actual_margin_needed': 72978.475456,
        'margin_ratio': 3932.841499943145
    },
    92.2: {
        'target_loss_percent': 92.2,
        'actual_margin_needed': 73233.806993,
        'margin_ratio': 3946.6014265336003
    },
    92.30000000000001: {
        'target_loss_percent': 92.30000000000001,
        'actual_margin_needed': 73490.485406,
        'margin_ratio': 3960.4339368522706
    },
    92.4: {
        'target_loss_percent': 92.4,
        'actual_margin_needed': 73748.448609,
        'margin_ratio': 3974.335685057854
    },
    92.5: {
        'target_loss_percent': 92.5,
        'actual_margin_needed': 74007.713598,
        'margin_ratio': 3988.3075870721163
    },
    92.60000000000001: {
        'target_loss_percent': 92.60000000000001,
        'actual_margin_needed': 74268.374052,
        'margin_ratio': 4002.354691296749
    },
    92.7: {
        'target_loss_percent': 92.7,
        'actual_margin_needed': 74530.296105,
        'margin_ratio': 4016.469783635308
    },
    92.80000000000001: {
        'target_loss_percent': 92.80000000000001,
        'actual_margin_needed': 74793.648502,
        'margin_ratio': 4030.6619578285818
    },
    92.9: {
        'target_loss_percent': 92.9,
        'actual_margin_needed': 75058.37446,
        'margin_ratio': 4044.928153816224
    },
    93.0: {
        'target_loss_percent': 93.0,
        'actual_margin_needed': 75324.49382,
        'margin_ratio': 4059.2694408382763
    },
    93.10000000000001: {
        'target_loss_percent': 93.10000000000001,
        'actual_margin_needed': 75592.097322,
        'margin_ratio': 4073.690708912453
    },
    93.2: {
        'target_loss_percent': 93.2,
        'actual_margin_needed': 75861.064945,
        'margin_ratio': 4088.1854900553294
    },
    93.30000000000001: {
        'target_loss_percent': 93.30000000000001,
        'actual_margin_needed': 76131.557016,
        'margin_ratio': 4102.762424358044
    },
    93.4: {
        'target_loss_percent': 93.4,
        'actual_margin_needed': 76403.5254,
        'margin_ratio': 4117.418917804697
    },
    93.5: {
        'target_loss_percent': 93.5,
        'actual_margin_needed': 76677.057801,
        'margin_ratio': 4132.1596968016565
    },
    93.60000000000001: {
        'target_loss_percent': 93.60000000000001,
        'actual_margin_needed': 76952.048772,
        'margin_ratio': 4146.979078764637
    },
    93.7: {
        'target_loss_percent': 93.7,
        'actual_margin_needed': 77228.649108,
        'margin_ratio': 4161.885190101189
    },
    93.80000000000001: {
        'target_loss_percent': 93.80000000000001,
        'actual_margin_needed': 77506.820093,
        'margin_ratio': 4176.875944389386
    },
    93.9: {
        'target_loss_percent': 93.9,
        'actual_margin_needed': 77786.587603,
        'margin_ratio': 4191.95273609802
    },
    94.0: {
        'target_loss_percent': 94.0,
        'actual_margin_needed': 78068.037124,
        'margin_ratio': 4207.12017210448
    },
    94.10000000000001: {
        'target_loss_percent': 94.10000000000001,
        'actual_margin_needed': 78351.077433,
        'margin_ratio': 4222.373336361975
    },
    94.2: {
        'target_loss_percent': 94.2,
        'actual_margin_needed': 78635.85419,
        'margin_ratio': 4237.720078550692
    },
    94.30000000000001: {
        'target_loss_percent': 94.30000000000001,
        'actual_margin_needed': 78922.395003,
        'margin_ratio': 4253.161886477651
    },
    94.4: {
        'target_loss_percent': 94.4,
        'actual_margin_needed': 79210.614205,
        'margin_ratio': 4268.694143511296
    },
    94.5: {
        'target_loss_percent': 94.5,
        'actual_margin_needed': 79500.657611,
        'margin_ratio': 4284.324707684829
    },
    94.60000000000001: {
        'target_loss_percent': 94.60000000000001,
        'actual_margin_needed': 79792.5551,
        'margin_ratio': 4300.0551891904415
    },
    94.7: {
        'target_loss_percent': 94.7,
        'actual_margin_needed': 80086.230052,
        'margin_ratio': 4315.881458943305
    },
    94.80000000000001: {
        'target_loss_percent': 94.80000000000001,
        'actual_margin_needed': 80381.878463,
        'margin_ratio': 4331.814079252345
    },
    94.9: {
        'target_loss_percent': 94.9,
        'actual_margin_needed': 80679.377812,
        'margin_ratio': 4347.846447408059
    },
    95.0: {
        'target_loss_percent': 95.0,
        'actual_margin_needed': 80978.867957,
        'margin_ratio': 4363.98610041835
    },
    95.10000000000001: {
        'target_loss_percent': 95.10000000000001,
        'actual_margin_needed': 81280.337561,
        'margin_ratio': 4380.232427327405
    },
    95.2: {
        'target_loss_percent': 95.2,
        'actual_margin_needed': 81583.874827,
        'margin_ratio': 4396.5901814329145
    },
    95.30000000000001: {
        'target_loss_percent': 95.30000000000001,
        'actual_margin_needed': 81889.47397,
        'margin_ratio': 4413.059050978731
    },
    95.4: {
        'target_loss_percent': 95.4,
        'actual_margin_needed': 82197.179445,
        'margin_ratio': 4429.641431663969
    },
    95.5: {
        'target_loss_percent': 95.5,
        'actual_margin_needed': 82507.082246,
        'margin_ratio': 4446.342227194513
    },
    95.60000000000001: {
        'target_loss_percent': 95.60000000000001,
        'actual_margin_needed': 82819.228002,
        'margin_ratio': 4463.163896536839
    },
    95.7: {
        'target_loss_percent': 95.7,
        'actual_margin_needed': 83133.579849,
        'margin_ratio': 4480.104453074098
    },
    95.80000000000001: {
        'target_loss_percent': 95.80000000000001,
        'actual_margin_needed': 83450.320031,
        'margin_ratio': 4497.173718014008
    },
    95.9: {
        'target_loss_percent': 95.9,
        'actual_margin_needed': 83769.41548,
        'margin_ratio': 4514.369909307793
    },
    96.0: {
        'target_loss_percent': 96.0,
        'actual_margin_needed': 84090.965276,
        'margin_ratio': 4531.698366419363
    },
    96.10000000000001: {
        'target_loss_percent': 96.10000000000001,
        'actual_margin_needed': 84414.989268,
        'margin_ratio': 4549.160159019883
    },
    96.2: {
        'target_loss_percent': 96.2,
        'actual_margin_needed': 84741.589899,
        'margin_ratio': 4566.760807806783
    },
    96.30000000000001: {
        'target_loss_percent': 96.30000000000001,
        'actual_margin_needed': 85070.832216,
        'margin_ratio': 4584.50381819093
    },
    96.4: {
        'target_loss_percent': 96.4,
        'actual_margin_needed': 85402.74958,
        'margin_ratio': 4602.390988010996
    },
    96.5: {
        'target_loss_percent': 96.5,
        'actual_margin_needed': 85737.452202,
        'margin_ratio': 4620.428256585275
    },
    96.60000000000001: {
        'target_loss_percent': 96.60000000000001,
        'actual_margin_needed': 86075.017595,
        'margin_ratio': 4638.6198011227525
    },
    96.7: {
        'target_loss_percent': 96.7,
        'actual_margin_needed': 86415.527564,
        'margin_ratio': 4656.97003013014
    },
    96.80000000000001: {
        'target_loss_percent': 96.80000000000001,
        'actual_margin_needed': 86759.068945,
        'margin_ratio': 4675.483623236907
    },
    96.9: {
        'target_loss_percent': 96.9,
        'actual_margin_needed': 87105.734296,
        'margin_ratio': 4694.165568433572
    },
    97.0: {
        'target_loss_percent': 97.0,
        'actual_margin_needed': 87455.593131,
        'margin_ratio': 4713.019611859559
    },
    97.10000000000001: {
        'target_loss_percent': 97.10000000000001,
        'actual_margin_needed': 87808.811455,
        'margin_ratio': 4732.054699595876
    },
    97.2: {
        'target_loss_percent': 97.2,
        'actual_margin_needed': 88165.471354,
        'margin_ratio': 4751.275255292444
    },
    97.30000000000001: {
        'target_loss_percent': 97.30000000000001,
        'actual_margin_needed': 88525.721296,
        'margin_ratio': 4770.689279953784
    },
    97.4: {
        'target_loss_percent': 97.4,
        'actual_margin_needed': 88889.637129,
        'margin_ratio': 4790.300861061307
    },
    97.5: {
        'target_loss_percent': 97.5,
        'actual_margin_needed': 89257.437408,
        'margin_ratio': 4810.121776638174
    },
    97.60000000000001: {
        'target_loss_percent': 97.60000000000001,
        'actual_margin_needed': 89629.197006,
        'margin_ratio': 4830.15606162263
    },
    97.7: {
        'target_loss_percent': 97.7,
        'actual_margin_needed': 90005.153035,
        'margin_ratio': 4850.41649408256
    },
    97.80000000000001: {
        'target_loss_percent': 97.80000000000001,
        'actual_margin_needed': 90385.435352,
        'margin_ratio': 4870.910072067673
    },
    97.9: {
        'target_loss_percent': 97.9,
        'actual_margin_needed': 90770.283008,
        'margin_ratio': 4891.6496781394
    },
    98.0: {
        'target_loss_percent': 98.0,
        'actual_margin_needed': 91159.885394,
        'margin_ratio': 4912.64551866037
    },
    98.10000000000001: {
        'target_loss_percent': 98.10000000000001,
        'actual_margin_needed': 91554.47767,
        'margin_ratio': 4933.910266504348
    },
    98.2: {
        'target_loss_percent': 98.2,
        'actual_margin_needed': 91954.337382,
        'margin_ratio': 4955.458878744913
    },
    98.30000000000001: {
        'target_loss_percent': 98.30000000000001,
        'actual_margin_needed': 92359.75129,
        'margin_ratio': 4977.306809002072
    },
    98.4: {
        'target_loss_percent': 98.4,
        'actual_margin_needed': 92771.056688,
        'margin_ratio': 4999.472234194879
    },
    98.5: {
        'target_loss_percent': 98.5,
        'actual_margin_needed': 93188.598795,
        'margin_ratio': 5021.97375832405
    },
    98.60000000000001: {
        'target_loss_percent': 98.60000000000001,
        'actual_margin_needed': 93612.817746,
        'margin_ratio': 5044.8351004544575
    },
    98.7: {
        'target_loss_percent': 98.7,
        'actual_margin_needed': 94044.180505,
        'margin_ratio': 5068.081425477348
    },
    98.80000000000001: {
        'target_loss_percent': 98.80000000000001,
        'actual_margin_needed': 94483.240295,
        'margin_ratio': 5091.742546818658
    },
    98.9: {
        'target_loss_percent': 98.9,
        'actual_margin_needed': 94930.639311,
        'margin_ratio': 5115.853072643762
    }
}

import pandas as pd
import re


def calculate_grid_atr_pct(df, day_list=[30, 90, 180, 365], timeframes=['1h'],
                           method_list=['trimmed']):
    """
    计算网格交易专用的不同时间维度、不同回溯天数下的 百分比ATR (ATR Percentage)。
    代表该时间维度下，单根K线的平均真实波动幅度百分比。
    """
    # 1. 数据预处理
    df_work = df.copy()
    # 转换 13位 毫秒级时间戳并设为索引
    df_work['open_time'] = pd.to_datetime(df_work['open_time'], unit='ms')
    df_work = df_work.set_index('open_time').sort_index()

    # 确保所需列为浮点数
    for col in ['high', 'low', 'close']:
        df_work[col] = df_work[col].astype(float)

    end_time = df_work.index[-1]

    # 时间单位映射 pandas rule
    def get_resample_rule(tf):
        match = re.match(r'(\d+)([a-zA-Z]+)', tf.lower())
        num, unit = match.group(1), match.group(2)
        if unit in ['m', 'min']: return f"{num}min"
        if unit in ['h', 'hour']: return f"{num}h"
        if unit in ['d', 'day']: return f"{num}D"
        raise ValueError(f"不支持的时间单位: {unit}")

    result = {}

    # 2. 遍历时间维度进行 K线重采样
    for tf in timeframes:
        rule = get_resample_rule(tf)

        # 聚合得到对应维度的 High, Low, Close
        resampled = df_work.resample(rule).agg({
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }).dropna()

        if resampled.empty:
            continue

        # 3. 计算真实波幅 True Range (TR)
        # TR = Max(High-Low, |High-PrevClose|, |Low-PrevClose|)
        prev_close = resampled['close'].shift(1)

        tr1 = resampled['high'] - resampled['low']
        tr2 = (resampled['high'] - prev_close).abs()
        tr3 = (resampled['low'] - prev_close).abs()

        # 按行取最大值得到 TR
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # 4. 计算 百分比 TR (TR / 前一根收盘价) * 100
        # 这样得到的是百分比，例如 0.5 表示单根K线波动 0.5%
        tr_pct = (tr / prev_close) * 100
        tr_pct = tr_pct.dropna()

        # 5. 根据天数截取并计算平均值 (即 ATR 的 A - Average)
        for days in day_list:
            start_time = end_time - pd.Timedelta(days=days)
            period_tr_pct = tr_pct.loc[start_time:end_time]

            if len(period_tr_pct) > 0:
                # 遍历每一种计算方法并存入字典
                for method in method_list:
                    key_name = f"atr_pct_{tf}_{days}d_{method}"

                    if method == 'mean':
                        typical_atr = period_tr_pct.mean()
                    elif method == 'median':
                        typical_atr = period_tr_pct.median()
                    elif method == 'trimmed':
                        lower_bound = period_tr_pct.quantile(0.0)
                        upper_bound = period_tr_pct.quantile(0.9)
                        filtered_tr = period_tr_pct[(period_tr_pct >= lower_bound) & (period_tr_pct <= upper_bound)]
                        typical_atr = filtered_tr.mean()
                    elif method.startswith('quantile_'):
                        q_val = float(method.split('_')[1]) / 100.0
                        typical_atr = period_tr_pct.quantile(q_val)
                    else:
                        raise ValueError(f"不支持的计算方法: {method}")

                    # 使用 float() 将 numpy 浮点数转为原生 python 浮点数，解决输出看着乱的问题
                    # 保留4位小数，例如 0.2531 代表平均波动 0.2531%
                    result[key_name] = round(float(typical_atr), 4)
            else:
                for method in method_list:
                    key_name = f"atr_pct_{tf}_{days}d_{method}"
                    result[key_name] = None

    return result

def get_closest_margin_info(target_value, margin_info):
    """
    根据目标值，在 margin_info 字典中找到 key 最接近的一项并返回。

    参数:
    target_value: 目标数值 (例如 1.18)
    margin_info: 包含配置信息的字典

    返回:
    最接近 key 对应的字典内容
    """
    if not margin_info:
        return {}

    # 核心逻辑：找出 keys 中，与 target_value 差值的绝对值(abs)最小的那个 key
    closest_key = min(margin_info.keys(), key=lambda k: abs(k - target_value))

    # 返回对应的字典内容
    return margin_info[closest_key]


if __name__ == "__main__":
    param_list = [
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\BTCUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\ETHUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\SOLUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\BNBUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\DOGEUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\LINKUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\TRXUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\AAVEUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\TONUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\SKYUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\UNIUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\STXUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\RENDERUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\RUNEUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\PENDLEUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\KASUSDT_1m_2025-01-01_merged.csv"
        }
    ]
    coin_info = {}
    atr_pct_result_list = []
    for param in param_list:
        csv_file_path = param["csv_file_path"]

        temp_df = pd.read_csv(csv_file_path)
        coin_name = os.path.basename(csv_file_path).split('_')[0]
        atr_pct_result = calculate_grid_atr_pct(temp_df)
        dd = dd_info_origin.get(coin_name, None)
        target_value = get_closest_margin_info(dd * 100, margin_info)
        margin_ratio = target_value.get('margin_ratio', None) if target_value else None
        # 将所有的atr_pct_result的除以margin_ratio，得到新的字段
        for key in list(atr_pct_result.keys()):
            if atr_pct_result[key] is not None and margin_ratio is not None:
                # 现在往字典里添加新 key 就没问题了
                atr_pct_result[f"score_{key}"] = round(atr_pct_result[key] / margin_ratio * 10000, 6)


        atr_pct_result['margin_ratio'] = margin_ratio
        atr_pct_result['dd'] = dd
        atr_pct_result['coin_name'] = coin_name
        atr_pct_result_list.append(atr_pct_result)

    # 将atr_pct_result_list转换为DataFrame
    atr_pct_df = pd.DataFrame(atr_pct_result_list)
    atr_pct_df.to_csv("grid_atr_pct_results.csv", index=False)
    print()


