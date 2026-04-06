ZONING_BY-LAW_569-2013_README

1. INSTRUCTIONS FOR OPTIMIZED VIEWING

To improve clarity, it is recommended to hide objects with a ZN_STATUS value of "5" in the ZONING_ZONE_CATAGORIES (Zoning Area) feature, as these are not part of Zoning By-law 569-2013.


2. DATASET FIELDS AND DESCRIPTIONS


**************************************
ZONING_ZONE_CATAGORIES
**************************************

Field name  (Description) [Notes]
======================================
OBJECTID = (Unique system identifier)
GEN_ZONE = (The land use category of the lands within the zone boundary. Each "zone category" has its own Chapter in the text of By-law 569-2013.)
ZN_ZONE = (The land use category of the lands within the zone boundary. Each "zone category" has its own Chapter in the text of By-law 569-2013.) [Zoned destination of the zone limited by GEN_ZONE. 
0 = Residential
1 = Open Space
2 = Utility and Transportation
4 = Employment Industrial
5 = Institutional
6 = Commercial Residential Employment
101 = Residential Apartment
201 = Commercial
202 = Commercial Residential]

ZN_HOLDING = (To indicate whether there is a HOLDING status for the zone. The zone label will be prefaced by the letter (H).  These are not common, and when used, a Holding Zone is most often applied to specific sites.) [Yes (Y) or No (N)]
HOLDING_ID (Holding Number if it exists.)
FRONTAGE = (The required minimum Lot Frontage in the zone, and is a numeric value prefaced by the letter "f" within a residential zone label.) [Unit = metres.]
ZN_AREA = (The required minimum Lot Area in the zone, and is a numeric value prefaced by the letter "a" within a residential zone.) [Unit = square metres]
UNITS = (The permitted maximum number of Dwelling Units allowed on a lot in the zone, and is a numeric value prefaced by the letter "u" in a residential zone.)
DENSITY =  (The permitted maximum Density in the zone by FSI (floor space index), and is a numeric value prefaced by the letter "d" in residential zones.)
COVERAGE = (The permitted maximum percentage of a lot that can be covered by any part of any building or structure on or above the surface of the lot. Unit = % (percent))
FSI_TOTAL = (The permitted maximum FSI (floor space index) Total in the zone. It is a numeric value following the zone symbol.)
PRCNT_COMM = (The permitted maximum FSI (floor space index) for Commercial Use in most mixed-use zones, and is a numeric value prefaced by letter "c" in the zone label.)
PRCNT_RES = (The permitted maximum FSI (floor space index) for Residential Use in most mixed-use zones, and is a numeric value prefaced by the letter "r" in the zone label.)
PRCNT_EMMP = (The permitted maximum FSI (floor space index) for Employment Uses in the zone, and is a numeric value prefaced by the letter "e" in the zone label.)
PRCNT_OFFC = (The permitted maximum FSI (floor space index) for Office Uses in an Employment-Office zone, and is a numeric value prefaced by the letter "o" in the zone label.)
ZN_EXCPTN = (This indicates whether a zone has an Exception.) [Yes (Y) or No (N)]
EXCPTN_NO = (This is the Exception Number for the zone if one exists.  The exception number is prefaced by the letter "x" in the zone label.  Each zone has its own series of exception numbers, starting at 1, so the exception number must be read in conjunction with the respective zone symbol.)
STAND_SET = (Set of standards referred to in the Commercial-Residential mixed use zone, based on three different design typologies.  The "standard set" number is prefaced by the letters "SS" in the zone label.)
ZN_STATUS = (Status of the Zone, primarily indicating whether the lands have been incorporated into By-law 569-2013 or not.) [0-4 and 6 = In the By-law. 5 = Not Part of Zoning By-law 569-2013]
ZN_STRING = (Complete label of the zone.)
AREA_UNITS = (This is also a type of density limit, indicating the required minimum lot area per dwelling unit on a lot in the zone.) [Unit = square metres]

ZBL_CHAPTR = (By-law text chapter number)
ZBL_SECTN = (By-law text section number)
ZBL_EXCPTN = (By-law text section number)


**************************************
ZONING_LOT_COVERAGE
**************************************

Field name  (Description) [Notes]
======================================
OBJECTID = (Unique system identifier)
PRCNT_CVER = (The permitted maximum percentage of a lot that can be covered by any part of any building or structure on or above the surface of the lot.) [Unit = % (percent)]


**************************************
ZONING_HEIGHT
**************************************

Field name  (Description) [Notes]
======================================
OBJECTID = (Unique system identifier)
HT_HEIGHT = (The permitted maximum Height, in metres.  A numeric value prefaced by the letters "HT".) [Unit = metres]
HT_STORIES = (The permitted maximum Height, in Storeys.  A numeric value prefaced by the letters "ST".) [Unit = Storeys, Negative values = 0]
HT_STRING = (Height in metres and Storeys) [Example: HT 10.5, ST 3]


**************************************
ZONING_POLICY_AREA
**************************************

Field name  (Description) [Notes]
======================================
POLICY_ID = (Policy Area)
ZN_EXCPTN = (Unused field)
EXCEPTN_ID = (Unused field)
CHAPT_200 = (Link to parking rate text)
EXCPTN_LK = (Link to parking exception text)


**************************************
ZONING_ROOMING_HOUSE
**************************************

Field name  (Description) [Notes]
======================================
RMH_AREA = (Rooming House Area.)
RMG_HS_NO = (Rooming House Number.  A sub-set of a Rooming House Area.)
RMG_STRING = (Rooming House Area + Number)  [Example: B1]
CHAP150_25 = (Link to Bylaw section) [See interactive web map]





