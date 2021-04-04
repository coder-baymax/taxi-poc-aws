from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, first, sum, lit
from pyspark.sql.types import DoubleType, StructType, IntegerType, StructField, StringType, LongType

# for local test
input_path = "/home/yunfei/aws/taxi-formatted/{}"
output_path = "/home/yunfei/aws/taxi-trip-record/{}"


# for s3 pre-treatment
# input_path = "s3://taxi-poc-formatted/{}"
# output_path = "s3://taxi-poc-trip-record/{}"


class Location:

    def __init__(self, location_id, borough, zone, lat, lng):
        self.location_id = location_id
        self.borough = borough
        self.zone = zone
        self.lat = lat
        self.lng = lng

    @property
    def json(self):
        return {
            "location_id": self.location_id,
            "borough": self.borough
        }


Locations = [
    Location(1, "EWR", "Newark Airport", 40.6895314, -74.1744624),
    Location(2, "Queens", "Jamaica Bay", 40.6056632, -73.8713099),
    Location(3, "Bronx", "Allerton/Pelham Gardens", 40.8627726, -73.84343919999999),
    Location(4, "Manhattan", "Alphabet City", 40.7258428, -73.9774916),
    Location(5, "Staten Island", "Arden Heights", 40.556413, -74.1735044),
    Location(6, "Staten Island", "Arrochar/Fort Wadsworth", 40.6012117, -74.0579185),
    Location(7, "Queens", "Astoria", 40.7643574, -73.92346189999999),
    Location(8, "Queens", "Astoria Park", 40.7785364, -73.92283359999999),
    Location(9, "Queens", "Auburndale", 40.7577672, -73.78339609999999),
    Location(10, "Queens", "Baisley Park", 40.6737751, -73.786025),
    Location(11, "Brooklyn", "Bath Beach", 40.6038852, -74.0062078),
    Location(12, "Manhattan", "Battery Park", 40.703141, -74.0159996),
    Location(13, "Manhattan", "Battery Park City", 40.7115786, -74.0158441),
    Location(14, "Brooklyn", "Bay Ridge", 40.6263732, -74.0298767),
    Location(15, "Queens", "Bay Terrace/Fort Totten", 40.7920899, -73.7760996),
    Location(16, "Queens", "Bayside", 40.7585569, -73.7654367),
    Location(17, "Brooklyn", "Bedford", 40.6872176, -73.9417735),
    Location(18, "Bronx", "Bedford Park", 40.8700999, -73.8856912),
    Location(19, "Queens", "Bellerose", 40.7361769, -73.7137365),
    Location(20, "Bronx", "Belmont", 40.8534507, -73.88936819999999),
    Location(21, "Brooklyn", "Bensonhurst East", 40.6139307, -73.9921833),
    Location(22, "Brooklyn", "Bensonhurst West", 40.6139307, -73.9921833),
    Location(23, "Staten Island", "Bloomfield/Emerson Hill", 40.6074525, -74.0963115),
    Location(24, "Manhattan", "Bloomingdale", 40.7988958, -73.9697795),
    Location(25, "Brooklyn", "Boerum Hill", 40.6848689, -73.9844722),
    Location(26, "Brooklyn", "Borough Park", 40.6350319, -73.9921028),
    Location(27, "Queens", "Breezy Point/Fort Tilden/Riis Beach", 40.5597687, -73.88761509999999),
    Location(28, "Queens", "Briarwood/Jamaica Hills", 40.7109315, -73.81356099999999),
    Location(29, "Brooklyn", "Brighton Beach", 40.5780706, -73.9596565),
    Location(30, "Queens", "Broad Channel", 40.6158335, -73.8213213),
    Location(31, "Bronx", "Bronx Park", 40.8608544, -73.8706278),
    Location(32, "Bronx", "Bronxdale", 40.8474697, -73.8599132),
    Location(33, "Brooklyn", "Brooklyn Heights", 40.6959294, -73.9955523),
    Location(34, "Brooklyn", "Brooklyn Navy Yard", 40.7025634, -73.9697795),
    Location(35, "Brooklyn", "Brownsville", 40.665214, -73.9125304),
    Location(36, "Brooklyn", "Bushwick North", 40.6957755, -73.9170604),
    Location(37, "Brooklyn", "Bushwick South", 40.7043655, -73.9383476),
    Location(38, "Queens", "Cambria Heights", 40.692158, -73.7330753),
    Location(39, "Brooklyn", "Canarsie", 40.6402325, -73.9060579),
    Location(40, "Brooklyn", "Carroll Gardens", 40.6795331, -73.9991637),
    Location(41, "Manhattan", "Central Harlem", 40.8089419, -73.9482305),
    Location(42, "Manhattan", "Central Harlem North", 40.8142585, -73.9426617),
    Location(43, "Manhattan", "Central Park", 40.7812199, -73.9665138),
    Location(44, "Staten Island", "Charleston/Tottenville", 40.5083408, -74.23554039999999),
    Location(45, "Manhattan", "Chinatown", 40.7157509, -73.9970307),
    Location(46, "Bronx", "City Island", 40.8468202, -73.7874983),
    Location(47, "Bronx", "Claremont/Bathgate", 40.84128339999999, -73.9001573),
    Location(48, "Manhattan", "Clinton East", 40.7637581, -73.9918181),
    Location(49, "Brooklyn", "Clinton Hill", 40.6896834, -73.9661144),
    Location(50, "Manhattan", "Clinton West", 40.7628785, -73.9940134),
    Location(51, "Bronx", "Co-Op City", 40.8738889, -73.82944440000001),
    Location(52, "Brooklyn", "Cobble Hill", 40.686536, -73.9962255),
    Location(53, "Queens", "College Point", 40.786395, -73.8389657),
    Location(54, "Brooklyn", "Columbia Street", 40.6775239, -74.00634409999999),
    Location(55, "Brooklyn", "Coney Island", 40.5755438, -73.9707016),
    Location(56, "Queens", "Corona", 40.7449859, -73.8642613),
    Location(57, "Queens", "Corona", 40.7449859, -73.8642613),
    Location(58, "Bronx", "Country Club", 40.8391667, -73.8197222),
    Location(59, "Bronx", "Crotona Park", 40.8400367, -73.8953489),
    Location(60, "Bronx", "Crotona Park East", 40.8365344, -73.8933509),
    Location(61, "Brooklyn", "Crown Heights North", 40.6694022, -73.9422324),
    Location(62, "Brooklyn", "Crown Heights South", 40.6694022, -73.9422324),
    Location(63, "Brooklyn", "Cypress Hills", 40.6836873, -73.87963309999999),
    Location(64, "Queens", "Douglaston", 40.76401509999999, -73.7433727),
    Location(65, "Brooklyn", "Downtown Brooklyn/MetroTech", 40.6930987, -73.98566339999999),
    Location(66, "Brooklyn", "DUMBO/Vinegar Hill", 40.70371859999999, -73.98226830000002),
    Location(67, "Brooklyn", "Dyker Heights", 40.6214932, -74.00958399999999),
    Location(68, "Manhattan", "East Chelsea", 40.7465004, -74.00137370000002),
    Location(69, "Bronx", "East Concourse/Concourse Village", 40.8255863, -73.9184388),
    Location(70, "Queens", "East Elmhurst", 40.7737505, -73.8713099),
    Location(71, "Brooklyn", "East Flatbush/Farragut", 40.63751329999999, -73.9280797),
    Location(72, "Brooklyn", "East Flatbush/Remsen Village", 40.6511399, -73.9181602),
    Location(73, "Queens", "East Flushing", 40.7540534, -73.8086418),
    Location(74, "Manhattan", "East Harlem North", 40.7957399, -73.93892129999999),
    Location(75, "Manhattan", "East Harlem South", 40.7957399, -73.93892129999999),
    Location(76, "Brooklyn", "East New York", 40.6590529, -73.8759245),
    Location(77, "Brooklyn", "East New York/Pennsylvania Avenue", 40.65845729999999, -73.8904498),
    Location(78, "Bronx", "East Tremont", 40.8453781, -73.8909693),
    Location(79, "Manhattan", "East Village", 40.7264773, -73.98153370000001),
    Location(80, "Brooklyn", "East Williamsburg", 40.7141953, -73.9316461),
    Location(81, "Bronx", "Eastchester", 40.8859837, -73.82794710000002),
    Location(82, "Queens", "Elmhurst", 40.737975, -73.8801301),
    Location(83, "Queens", "Elmhurst/Maspeth", 40.7294018, -73.9065883),
    Location(84, "Staten Island", "Eltingville/Annadale/Prince's Bay", 40.52899439999999, -74.197644),
    Location(85, "Brooklyn", "Erasmus", 40.649649, -73.95287379999999),
    Location(86, "Queens", "Far Rockaway", 40.5998931, -73.74484369999999),
    Location(87, "Manhattan", "Financial District North", 40.7077143, -74.00827869999999),
    Location(88, "Manhattan", "Financial District South", 40.705123, -74.0049259),
    Location(89, "Brooklyn", "Flatbush/Ditmas Park", 40.6414876, -73.9593998),
    Location(90, "Manhattan", "Flatiron", 40.740083, -73.9903489),
    Location(91, "Brooklyn", "Flatlands", 40.6232714, -73.9321664),
    Location(92, "Queens", "Flushing", 40.7674987, -73.833079),
    Location(93, "Queens", "Flushing Meadows-Corona Park", 40.7400275, -73.8406953),
    Location(94, "Bronx", "Fordham South", 40.8592667, -73.8984694),
    Location(95, "Queens", "Forest Hills", 40.718106, -73.8448469),
    Location(96, "Queens", "Forest Park/Highland Park", 40.6960418, -73.8663024),
    Location(97, "Brooklyn", "Fort Greene", 40.6920638, -73.97418739999999),
    Location(98, "Queens", "Fresh Meadows", 40.7335179, -73.7801447),
    Location(99, "Staten Island", "Freshkills Park", 40.5772365, -74.1858183),
    Location(100, "Manhattan", "Garment District", 40.7547072, -73.9916342),
    Location(101, "Queens", "Glen Oaks", 40.7471504, -73.7118223),
    Location(102, "Queens", "Glendale", 40.7016662, -73.8842219),
    Location(103, "Manhattan", "Governor's Island/Ellis Island/Liberty Island", 40.6892494, -74.04450039999999),
    Location(104, "Manhattan", "Governor's Island/Ellis Island/Liberty Island", 40.6892494, -74.04450039999999),
    Location(105, "Manhattan", "Governor's Island/Ellis Island/Liberty Island", 40.6892494, -74.04450039999999),
    Location(106, "Brooklyn", "Gowanus", 40.6751161, -73.9879753),
    Location(107, "Manhattan", "Gramercy", 40.7367783, -73.9844722),
    Location(108, "Brooklyn", "Gravesend", 40.5918636, -73.9768653),
    Location(109, "Staten Island", "Great Kills", 40.5543273, -74.156292),
    Location(110, "Staten Island", "Great Kills Park", 40.5492367, -74.1238486),
    Location(111, "Brooklyn", "Green-Wood Cemetery", 40.6579777, -73.9940634),
    Location(112, "Brooklyn", "Greenpoint", 40.7304701, -73.95150319999999),
    Location(113, "Manhattan", "Greenwich Village North", 40.7335719, -74.0027418),
    Location(114, "Manhattan", "Greenwich Village South", 40.7335719, -74.0027418),
    Location(115, "Staten Island", "Grymes Hill/Clifton", 40.6189726, -74.0784785),
    Location(116, "Manhattan", "Hamilton Heights", 40.8252793, -73.94761390000001),
    Location(117, "Queens", "Hammels/Arverne", 40.5880813, -73.81199289999999),
    Location(118, "Staten Island", "Heartland Village/Todt Hill", 40.5975007, -74.10189749999999),
    Location(119, "Bronx", "Highbridge", 40.836916, -73.9271294),
    Location(120, "Manhattan", "Highbridge Park", 40.8537599, -73.9257492),
    Location(121, "Queens", "Hillcrest/Pomonok", 40.732341, -73.81077239999999),
    Location(122, "Queens", "Hollis", 40.7112203, -73.762495),
    Location(123, "Brooklyn", "Homecrest", 40.6004787, -73.9565551),
    Location(124, "Queens", "Howard Beach", 40.6571222, -73.8429989),
    Location(125, "Manhattan", "Hudson Sq", 40.7265834, -74.0074731),
    Location(126, "Bronx", "Hunts Point", 40.8094385, -73.8803315),
    Location(127, "Manhattan", "Inwood", 40.8677145, -73.9212019),
    Location(128, "Manhattan", "Inwood Hill Park", 40.8722007, -73.9255549),
    Location(129, "Queens", "Jackson Heights", 40.7556818, -73.8830701),
    Location(130, "Queens", "Jamaica", 40.702677, -73.7889689),
    Location(131, "Queens", "Jamaica Estates", 40.7179512, -73.783822),
    Location(132, "Queens", "JFK Airport", 40.6413111, -73.77813909999999),
    Location(133, "Brooklyn", "Kensington", 40.63852019999999, -73.97318729999999),
    Location(134, "Queens", "Kew Gardens", 40.705695, -73.8272029),
    Location(135, "Queens", "Kew Gardens Hills", 40.724707, -73.8207618),
    Location(136, "Bronx", "Kingsbridge Heights", 40.8711235, -73.8976328),
    Location(137, "Manhattan", "Kips Bay", 40.74232920000001, -73.9800645),
    Location(138, "Queens", "LaGuardia Airport", 40.7769271, -73.8739659),
    Location(139, "Queens", "Laurelton", 40.67764, -73.7447853),
    Location(140, "Manhattan", "Lenox Hill East", 40.7662315, -73.9602312),
    Location(141, "Manhattan", "Lenox Hill West", 40.7662315, -73.9602312),
    Location(142, "Manhattan", "Lincoln Square East", 40.7741769, -73.98491179999999),
    Location(143, "Manhattan", "Lincoln Square West", 40.7741769, -73.98491179999999),
    Location(144, "Manhattan", "Little Italy/NoLiTa", 40.7230413, -73.99486069999999),
    Location(145, "Queens", "Long Island City/Hunters Point", 40.7485587, -73.94964639999999),
    Location(146, "Queens", "Long Island City/Queens Plaza", 40.7509846, -73.9402762),
    Location(147, "Bronx", "Longwood", 40.8248438, -73.8915875),
    Location(148, "Manhattan", "Lower East Side", 40.715033, -73.9842724),
    Location(149, "Brooklyn", "Madison", 40.60688529999999, -73.947958),
    Location(150, "Brooklyn", "Manhattan Beach", 40.57815799999999, -73.93892129999999),
    Location(151, "Manhattan", "Manhattan Valley", 40.7966989, -73.9684247),
    Location(152, "Manhattan", "Manhattanville", 40.8169443, -73.9558333),
    Location(153, "Manhattan", "Marble Hill", 40.8761173, -73.9102628),
    Location(154, "Brooklyn", "Marine Park/Floyd Bennett Field", 40.58816030000001, -73.8969745),
    Location(155, "Brooklyn", "Marine Park/Mill Basin", 40.6055157, -73.9348698),
    Location(156, "Staten Island", "Mariners Harbor", 40.63677010000001, -74.1587547),
    Location(157, "Queens", "Maspeth", 40.7294018, -73.9065883),
    Location(158, "Manhattan", "Meatpacking/West Village West", 40.7342331, -74.0100622),
    Location(159, "Bronx", "Melrose South", 40.824545, -73.9104143),
    Location(160, "Queens", "Middle Village", 40.717372, -73.87425),
    Location(161, "Manhattan", "Midtown Center", 40.7314658, -73.9970956),
    Location(162, "Manhattan", "Midtown East", 40.7571432, -73.9718815),
    Location(163, "Manhattan", "Midtown North", 40.7649516, -73.9851039),
    Location(164, "Manhattan", "Midtown South", 40.7521795, -73.9875438),
    Location(165, "Brooklyn", "Midwood", 40.6204388, -73.95997779999999),
    Location(166, "Manhattan", "Morningside Heights", 40.8105443, -73.9620581),
    Location(167, "Bronx", "Morrisania/Melrose", 40.824545, -73.9104143),
    Location(168, "Bronx", "Mott Haven/Port Morris", 40.8022025, -73.9166051),
    Location(169, "Bronx", "Mount Hope", 40.8488863, -73.9051185),
    Location(170, "Manhattan", "Murray Hill", 40.7478792, -73.9756567),
    Location(171, "Queens", "Murray Hill-Queens", 40.7634996, -73.8073261),
    Location(172, "Staten Island", "New Dorp/Midland Beach", 40.5739937, -74.1159755),
    Location(173, "Queens", "North Corona", 40.7543725, -73.8669188),
    Location(174, "Bronx", "Norwood", 40.8810341, -73.878486),
    Location(175, "Queens", "Oakland Gardens", 40.7408584, -73.758241),
    Location(176, "Staten Island", "Oakwood", 40.563994, -74.1159754),
    Location(177, "Brooklyn", "Ocean Hill", 40.6782737, -73.9108212),
    Location(178, "Brooklyn", "Ocean Parkway South", 40.61287799999999, -73.96838620000001),
    Location(179, "Queens", "Old Astoria", 40.7643574, -73.92346189999999),
    Location(180, "Queens", "Ozone Park", 40.6794072, -73.8507279),
    Location(181, "Brooklyn", "Park Slope", 40.6710672, -73.98142279999999),
    Location(182, "Bronx", "Parkchester", 40.8382522, -73.8566087),
    Location(183, "Bronx", "Pelham Bay", 40.8505556, -73.83333329999999),
    Location(184, "Bronx", "Pelham Bay Park", 40.8670144, -73.81006339999999),
    Location(185, "Bronx", "Pelham Parkway", 40.8553279, -73.8639594),
    Location(186, "Manhattan", "Penn Station/Madison Sq West", 40.7505045, -73.9934387),
    Location(187, "Staten Island", "Port Richmond", 40.63549140000001, -74.1254641),
    Location(188, "Brooklyn", "Prospect-Lefferts Gardens", 40.6592355, -73.9533895),
    Location(189, "Brooklyn", "Prospect Heights", 40.6774196, -73.9668408),
    Location(190, "Brooklyn", "Prospect Park", 40.6602037, -73.9689558),
    Location(191, "Queens", "Queens Village", 40.7156628, -73.7419017),
    Location(192, "Queens", "Queensboro Hill", 40.7429383, -73.8251741),
    Location(193, "Queens", "Queensbridge/Ravenswood", 40.7556711, -73.9456723),
    Location(194, "Manhattan", "Randalls Island", 40.7932271, -73.92128579999999),
    Location(195, "Brooklyn", "Red Hook", 40.6733676, -74.00831889999999),
    Location(196, "Queens", "Rego Park", 40.72557219999999, -73.8624893),
    Location(197, "Queens", "Richmond Hill", 40.6958108, -73.8272029),
    Location(198, "Queens", "Ridgewood", 40.7043986, -73.9018292),
    Location(199, "Bronx", "Rikers Island", 40.79312770000001, -73.88601),
    Location(200, "Bronx", "Riverdale/North Riverdale/Fieldston", 40.89961830000001, -73.9088276),
    Location(201, "Queens", "Rockaway Park", 40.57978629999999, -73.8372237),
    Location(202, "Manhattan", "Roosevelt Island", 40.76050310000001, -73.9509934),
    Location(203, "Queens", "Rosedale", 40.6584068, -73.7389596),
    Location(204, "Staten Island", "Rossville/Woodrow", 40.5434385, -74.19764409999999),
    Location(205, "Queens", "Saint Albans", 40.6895283, -73.76436880000001),
    Location(206, "Staten Island", "Saint George/New Brighton", 40.6404369, -74.090226),
    Location(207, "Queens", "Saint Michaels Cemetery/Woodside", 40.7646761, -73.89850419999999),
    Location(208, "Bronx", "Schuylerville/Edgewater Park", 40.8235967, -73.81029269999999),
    Location(209, "Manhattan", "Seaport", 40.70722629999999, -74.0027431),
    Location(210, "Brooklyn", "Sheepshead Bay", 40.5953955, -73.94575379999999),
    Location(211, "Manhattan", "SoHo", 40.723301, -74.0029883),
    Location(212, "Bronx", "Soundview/Bruckner", 40.8247566, -73.8710929),
    Location(213, "Bronx", "Soundview/Castle Hill", 40.8176831, -73.8507279),
    Location(214, "Staten Island", "South Beach/Dongan Hills", 40.5903824, -74.06680759999999),
    Location(215, "Queens", "South Jamaica", 40.6808594, -73.7919103),
    Location(216, "Queens", "South Ozone Park", 40.6764003, -73.8124984),
    Location(217, "Brooklyn", "South Williamsburg", 40.7043921, -73.9565551),
    Location(218, "Queens", "Springfield Gardens North", 40.6715916, -73.779798),
    Location(219, "Queens", "Springfield Gardens South", 40.6715916, -73.779798),
    Location(220, "Bronx", "Spuyten Duyvil/Kingsbridge", 40.8833912, -73.9051185),
    Location(221, "Staten Island", "Stapleton", 40.6264929, -74.07764139999999),
    Location(222, "Brooklyn", "Starrett City", 40.6484272, -73.88236119999999),
    Location(223, "Queens", "Steinway", 40.7745459, -73.9037477),
    Location(224, "Manhattan", "Stuy Town/Peter Cooper Village", 40.7316903, -73.9778494),
    Location(225, "Brooklyn", "Stuyvesant Heights", 40.6824166, -73.9319933),
    Location(226, "Queens", "Sunnyside", 40.7432759, -73.9196324),
    Location(227, "Brooklyn", "Sunset Park East", 40.65272, -74.00933479999999),
    Location(228, "Brooklyn", "Sunset Park West", 40.65272, -74.00933479999999),
    Location(229, "Manhattan", "Sutton Place/Turtle Bay North", 40.7576281, -73.961698),
    Location(230, "Manhattan", "Times Sq/Theatre District", 40.759011, -73.9844722),
    Location(231, "Manhattan", "TriBeCa/Civic Center", 40.71625299999999, -74.0122396),
    Location(232, "Manhattan", "Two Bridges/Seward Park", 40.7149056, -73.98924699999999),
    Location(233, "Manhattan", "UN/Turtle Bay South", 40.7571432, -73.9718815),
    Location(234, "Manhattan", "Union Sq", 40.7358633, -73.9910835),
    Location(235, "Bronx", "University Heights/Morris Heights", 40.8540855, -73.9198498),
    Location(236, "Manhattan", "Upper East Side North", 40.7600931, -73.9598414),
    Location(237, "Manhattan", "Upper East Side South", 40.7735649, -73.9565551),
    Location(238, "Manhattan", "Upper West Side North", 40.7870106, -73.9753676),
    Location(239, "Manhattan", "Upper West Side South", 40.7870106, -73.9753676),
    Location(240, "Bronx", "Van Cortlandt Park", 40.8972233, -73.8860668),
    Location(241, "Bronx", "Van Cortlandt Village", 40.8837203, -73.89313899999999),
    Location(242, "Bronx", "Van Nest/Morris Park", 40.8459682, -73.8625946),
    Location(243, "Manhattan", "Washington Heights North", 40.852476, -73.9342996),
    Location(244, "Manhattan", "Washington Heights South", 40.8417082, -73.9393554),
    Location(245, "Staten Island", "West Brighton", 40.6270298, -74.10931409999999),
    Location(246, "Manhattan", "West Chelsea/Hudson Yards", 40.7542535, -74.0023331),
    Location(247, "Bronx", "West Concourse", 40.8316761, -73.9227554),
    Location(248, "Bronx", "West Farms/Bronx River", 40.8430609, -73.8816001),
    Location(249, "Manhattan", "West Village", 40.73468, -74.0047554),
    Location(250, "Bronx", "Westchester Village/Unionport", 40.8340447, -73.8531349),
    Location(251, "Staten Island", "Westerleigh", 40.616296, -74.1386767),
    Location(252, "Queens", "Whitestone", 40.7920449, -73.8095574),
    Location(253, "Queens", "Willets Point", 40.7606911, -73.840436),
    Location(254, "Bronx", "Williamsbridge/Olinville", 40.8787602, -73.85283559999999),
    Location(255, "Brooklyn", "Williamsburg (North Side)", 40.71492, -73.9528472),
    Location(256, "Brooklyn", "Williamsburg (South Side)", 40.70824229999999, -73.9571487),
    Location(257, "Brooklyn", "Windsor Terrace", 40.6539346, -73.9756567),
    Location(258, "Queens", "Woodhaven", 40.6901366, -73.8566087),
    Location(259, "Bronx", "Woodlawn/Wakefield", 40.8955885, -73.8627133),
    Location(260, "Queens", "Woodside", 40.7532952, -73.9068973),
    Location(261, "Manhattan", "World Trade Center", 40.7118011, -74.0131196),
    Location(262, "Manhattan", "Yorkville East", 40.7762231, -73.94920789999999),
    Location(263, "Manhattan", "Yorkville West", 40.7762231, -73.94920789999999)
]

fields = [
    ("vendor_type", IntegerType),
    ("passenger_count", IntegerType),
    ("total_amount", DoubleType),
    ("trip_distance", DoubleType),
    ("pickup_latitude", DoubleType),
    ("pickup_longitude", DoubleType),
    ("dropoff_latitude", DoubleType),
    ("dropoff_longitude", DoubleType)
]

location_schema = StructType([
    StructField("location_id", IntegerType(), False),
    StructField("borough", StringType(), False),
])

counter_schema = StructType([
    StructField("level", StringType(), False),
    StructField("total", DoubleType(), False),
    StructField("count", IntegerType(), False),
])


def parse_int(num):
    try:
        return int(num)
    except (ValueError, TypeError):
        return None


def parse_float(num):
    try:
        return float(num)
    except (ValueError, TypeError):
        return None


def parse_location(lat, lng):
    if not lat or not lng:
        return -1, "NA"
    min_loc, min_value = None, 0xffffffff
    for location in Locations:
        value = (lat - location.lat) ** 2 + (lng - location.lng) ** 2
        if value < min_value:
            min_loc = location
            min_value = value
    return min_loc.location_id, min_loc.borough


def parse_amount(total_amount):
    if total_amount:
        amount_level = "<20" if total_amount < 20 else "20-40" if total_amount <= 40 else ">40"
        amount_total = total_amount
        amount_count = 1
    else:
        amount_level = "NA"
        amount_total = 0
        amount_count = 0
    return amount_level, amount_total, amount_count


def parse_distance(trip_distance):
    if trip_distance:
        distance_level = "<10" if trip_distance < 10 else "10-20" if trip_distance <= 20 else ">20"
        distance_total = trip_distance
        distance_count = 1
    else:
        distance_level = "NA"
        distance_total = 0
        distance_count = 0
    return distance_level, distance_total, distance_count


parse_int_udf = udf(parse_int, IntegerType())
parse_float_udf = udf(parse_float, DoubleType())
parse_time_udf = udf(lambda x: int(datetime(x.year, x.month, x.day).timestamp() * 1000), LongType())
parse_location_udf = udf(parse_location, location_schema)
parse_amount_udf = udf(parse_amount, counter_schema)
parse_distance_udf = udf(parse_distance, counter_schema)


def agg_df(df):
    df = df.withColumn("timestamp", parse_time_udf(df.date))

    for field_name, field_type in fields:
        if field_type == IntegerType:
            df = df.withColumn(field_name, parse_int_udf(col(field_name)))
        elif field_type == DoubleType:
            df = df.withColumn(field_name, parse_float_udf(col(field_name)))

    df = df.withColumn("location", parse_location_udf(df.pickup_latitude, df.pickup_longitude)) \
        .withColumn("amount", parse_amount_udf(df.total_amount)) \
        .withColumn("distance", parse_amount_udf(df.trip_distance))

    df = df.withColumn("location_id", df.location.location_id).withColumn("location_borough", df.location.borough) \
        .withColumn("amount_level", df.amount.level).withColumn("amount_total", df.amount.total) \
        .withColumn("amount_count", df.amount.count) \
        .withColumn("distance_level", df.distance.level).withColumn("distance_total", df.distance.total) \
        .withColumn("distance_count", df.distance.count) \
        .withColumn("record_count", lit(1))

    df = df.groupBy("date", "vendor_type", "location_id", "amount_level", "distance_level").agg(
        first("timestamp").alias("timestamp"),
        first("location_borough").alias("location_borough"),
        sum("amount_total").alias("amount_total"),
        sum("amount_count").alias("amount_count"),
        sum("distance_total").alias("distance_total"),
        sum("distance_count").alias("distance_count"),
        sum("record_count").alias("record_count"),
    )

    return df.select("date", "timestamp", "vendor_type", "location_id", "location_borough", "amount_level",
                     "amount_total", "amount_count", "distance_level", "distance_total", "distance_count",
                     "record_count")


def agg_result_from_formatted():
    spark = SparkSession.builder.getOrCreate()

    for year in range(2009, 2021):
        for month in range(1, 13):
            month_str = "%d-%02d" % (year, month)
            df = agg_df(spark.read.option("header", True).csv(input_path.format(month_str)))
            df.repartition("date").write.partitionBy("date").mode("overwrite").format("json").save(
                output_path.format(month_str))

    spark.stop()


if __name__ == '__main__':
    agg_result_from_formatted()
