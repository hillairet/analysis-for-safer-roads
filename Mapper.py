Vehicle_Types ={1:'Bicycle',
                2:'Moped <50cm3',
                3:'Motorised quadricycle',
                4:'Car',
                5:'Light commercial vehicle',
                6:'Large goods vehicle alone 3.5T< GVWR <=7.5T',
                7:'Large goods vehicle alone 7.5T> GVWR',
                8:'Large goods vehicle +trailer 3.5T> GVWR',
                9:'Tractor unit alone',
                10:'Tractor unit with trailer',
                11:'Oversize load',
                12:'Agric. tractor',
                13:'Motor scooter <50cm3',
                14:'Motorcycle >50cm3 and <=125cm3',
                15:'Motor scooter >50cm3 and <=125cm3',
                16:'Motorcycle >125cm3',
                17:'Motor scooter >125cm3',
                18:'Quad bike <=50cm3',
                19:'Quad bike >50cm3',
                20:'Bus',
                21:'Coach (bus)',
                22:'Train',
                23:'Tramway',
                24:'Other'}

# Average vehicle weight in kg
Vehicle_Weights ={1:80,             # Average weight adult ~70kg + bike weight
                2:180,              # Average weight moped ~90kg + adult weight
                3:550,
                4:1200,
                5:2500,
                6:5500,
                7:9500,
                8:11000,
                9:5500,
                10:15000,
                11:30000,
                12:4000,
                13:160,             # Average weight scooter 90kg + adult weight
                14:220,             # Average weight motorcyle 150kg + adult weight
                15:190,             # Average weight scooter 120kg + adult weight
                16:270,             # Average weight motorcyle 200kg + adult weight
                17:210,             # Average weight scooter 140kg + adult weight
                18:170,             # Average weight quad ~100kg + adult weight
                19:270,             # Average weight quad ~200kg + adult weight
                20:14000,
                21:17000,
                22:200000,          # Basically just a large number
                23:60000,
                24:5000}            # Assumption that the other vehicles are rather heavy
