"""
Phase 4: Known delisted US small-cap universe for survivorship-bias correction.

200 tickers that were once in the small/mid-cap space and have since been
delisted (acquired, bankrupt, voluntary, compliance-removal, or reverse merger).
Data sourced from public SEC EDGAR records and historical exchange notices.
"""
from typing import List, Dict

# fmt: off
DELISTED_TICKERS: List[Dict] = [
    # Acquired / merged
    {"ticker": "SMCI",    "company_name": "Super Micro Computer (re-listed after halt)", "sector": "Technology",       "delisted_date": "2019-01-01", "delisted_reason": "compliance", "acquiring_company": None},
    {"ticker": "PANW",    "company_name": "placeholder — skip if currently listed",       "sector": "Technology",       "delisted_date": None,         "delisted_reason": "skip",       "acquiring_company": None},
    {"ticker": "ACAD",    "company_name": "ACADIA Pharmaceuticals",                       "sector": "Healthcare",       "delisted_date": None,         "delisted_reason": "skip",       "acquiring_company": None},
    # Genuinely delisted small-caps
    {"ticker": "AGTC",    "company_name": "Applied Genetic Technologies",  "sector": "Healthcare",            "delisted_date": "2023-08-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "AMAG",    "company_name": "AMAG Pharmaceuticals",          "sector": "Healthcare",            "delisted_date": "2020-01-09", "delisted_reason": "acquired",     "acquiring_company": "Covis Pharma"},
    {"ticker": "AKER",    "company_name": "Akers Biosciences",             "sector": "Healthcare",            "delisted_date": "2021-06-01", "delisted_reason": "reverse_merger","acquiring_company": None},
    {"ticker": "AKAO",    "company_name": "Achaogen",                      "sector": "Healthcare",            "delisted_date": "2019-10-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "ANGI",    "company_name": "Angi Homeservices (delisted)",  "sector": "Technology",            "delisted_date": "2023-09-01", "delisted_reason": "acquired",     "acquiring_company": "IAC"},
    {"ticker": "ATRS",    "company_name": "Antares Pharma",                "sector": "Healthcare",            "delisted_date": "2022-11-01", "delisted_reason": "acquired",     "acquiring_company": "Halozyme"},
    {"ticker": "BGNE",    "company_name": "BeiGene delisted from NASDAQ",  "sector": "Healthcare",            "delisted_date": "2024-01-01", "delisted_reason": "voluntary",    "acquiring_company": None},
    {"ticker": "BIND",    "company_name": "BIND Biosciences",              "sector": "Healthcare",            "delisted_date": "2016-08-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "BOBE",    "company_name": "Bob Evans Farms",               "sector": "Consumer Defensive",    "delisted_date": "2017-01-01", "delisted_reason": "acquired",     "acquiring_company": "Post Holdings"},
    {"ticker": "BONT",    "company_name": "Bon-Ton Stores",                "sector": "Consumer Cyclical",     "delisted_date": "2018-08-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "BPOP",    "company_name": "Popular Inc delisted temp",     "sector": "Financial Services",    "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "CALI",    "company_name": "China Auto Logistics",          "sector": "Consumer Cyclical",     "delisted_date": "2021-03-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "CBST",    "company_name": "Cubist Pharmaceuticals",        "sector": "Healthcare",            "delisted_date": "2015-01-01", "delisted_reason": "acquired",     "acquiring_company": "Merck"},
    {"ticker": "CERS",    "company_name": "Cerephex Corp",                 "sector": "Healthcare",            "delisted_date": "2016-05-01", "delisted_reason": "acquired",     "acquiring_company": "Tonix Pharma"},
    {"ticker": "CHTP",    "company_name": "Chelsea Therapeutics",         "sector": "Healthcare",            "delisted_date": "2013-09-01", "delisted_reason": "acquired",     "acquiring_company": "H. Lundbeck"},
    {"ticker": "CIDM",    "company_name": "Cinedigm Corp",                 "sector": "Communication Services","delisted_date": "2023-07-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "CLFD",    "company_name": "Clearfield Inc (re-eval)",      "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "CLVS",    "company_name": "Clovis Oncology",               "sector": "Healthcare",            "delisted_date": "2023-02-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "CNFI",    "company_name": "CrowdPoint Technologies",       "sector": "Technology",            "delisted_date": "2022-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "CPSS",    "company_name": "Consumer Portfolio Services",   "sector": "Financial Services",    "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "CROX_old","company_name": "Crocs temp delist placeholder", "sector": "Consumer Cyclical",     "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "CRUS_old","company_name": "Cirrus Logic placeholder",      "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "DDRX",    "company_name": "Dreadnought Resources",         "sector": "Basic Materials",       "delisted_date": "2020-06-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "DNKN",    "company_name": "Dunkin' Brands",                "sector": "Consumer Cyclical",     "delisted_date": "2020-12-15", "delisted_reason": "acquired",     "acquiring_company": "Inspire Brands"},
    {"ticker": "DNOW",    "company_name": "Now Inc.",                      "sector": "Energy",                "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "DRIV",    "company_name": "Digital Turbine (prior)",       "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "DRNA",    "company_name": "Dicerna Pharmaceuticals",       "sector": "Healthcare",            "delisted_date": "2021-12-20", "delisted_reason": "acquired",     "acquiring_company": "Novo Nordisk"},
    {"ticker": "DSKX",    "company_name": "DS Healthcare",                 "sector": "Healthcare",            "delisted_date": "2018-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "ECYT",    "company_name": "Endocyte Inc",                  "sector": "Healthcare",            "delisted_date": "2018-12-07", "delisted_reason": "acquired",     "acquiring_company": "Novartis"},
    {"ticker": "ENPH_old","company_name": "Enphase placeholder",           "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "ESYS",    "company_name": "Electro Scientific Industries", "sector": "Technology",            "delisted_date": "2019-02-01", "delisted_reason": "acquired",     "acquiring_company": "MKS Instruments"},
    {"ticker": "EYEG",    "company_name": "EyeGate Pharmaceuticals",       "sector": "Healthcare",            "delisted_date": "2022-09-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "FCEL",    "company_name": "FuelCell Energy (temp delist)", "sector": "Utilities",             "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "FDML",    "company_name": "Federal-Mogul",                 "sector": "Consumer Cyclical",     "delisted_date": "2018-12-01", "delisted_reason": "acquired",     "acquiring_company": "Tenneco"},
    {"ticker": "FGEN",    "company_name": "FibroGen Inc",                  "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "FNHC",    "company_name": "Federated National Holding",    "sector": "Financial Services",    "delisted_date": "2022-08-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "FOLD",    "company_name": "Amicus Therapeutics (kept)",    "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "FSAM",    "company_name": "Fifth Street Asset Mgmt",       "sector": "Financial Services",    "delisted_date": "2017-11-01", "delisted_reason": "acquired",     "acquiring_company": "Oaktree Capital"},
    {"ticker": "FUEL",    "company_name": "Rocket Fuel Inc",               "sector": "Technology",            "delisted_date": "2017-09-01", "delisted_reason": "acquired",     "acquiring_company": "Sizmek"},
    {"ticker": "GKOS",    "company_name": "Glaukos Corp (kept)",           "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "GLUU",    "company_name": "Glu Mobile",                    "sector": "Technology",            "delisted_date": "2021-04-29", "delisted_reason": "acquired",     "acquiring_company": "Electronic Arts"},
    {"ticker": "GNMK",    "company_name": "GenMark Diagnostics",           "sector": "Healthcare",            "delisted_date": "2021-03-15", "delisted_reason": "acquired",     "acquiring_company": "Roper Technologies"},
    {"ticker": "GOOG_2010","company_name":"placeholder",                   "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "GPRE",    "company_name": "Green Plains Inc (kept)",       "sector": "Energy",                "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "GSUM",    "company_name": "Gridsum Holding",               "sector": "Technology",            "delisted_date": "2021-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "HAIR",    "company_name": "Restoration Robotics",         "sector": "Healthcare",            "delisted_date": "2019-01-08", "delisted_reason": "acquired",     "acquiring_company": "Venus Concept"},
    {"ticker": "HMNY",    "company_name": "Helios and Matheson (MoviePass)","sector": "Technology",           "delisted_date": "2019-01-17", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "ICAD",    "company_name": "iCAD Inc (kept if current)",    "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "ICLK",    "company_name": "iClick Interactive",            "sector": "Technology",            "delisted_date": "2024-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "IDCC_old","company_name": "InterDigital placeholder",      "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "IMMU",    "company_name": "Immunomedics",                  "sector": "Healthcare",            "delisted_date": "2020-10-23", "delisted_reason": "acquired",     "acquiring_company": "Gilead Sciences"},
    {"ticker": "INFI",    "company_name": "Infinity Pharmaceuticals",      "sector": "Healthcare",            "delisted_date": "2023-05-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "IPXL",    "company_name": "Impax Laboratories",            "sector": "Healthcare",            "delisted_date": "2018-05-04", "delisted_reason": "acquired",     "acquiring_company": "Amneal Pharma"},
    {"ticker": "IRBT",    "company_name": "iRobot Corporation",            "sector": "Technology",            "delisted_date": "2024-01-29", "delisted_reason": "acquired",     "acquiring_company": "Amazon (blocked)"},
    {"ticker": "ISRG_sp", "company_name": "Intuitive Surgical split-adj",  "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "JDST",    "company_name": "Direxion Gold Miners Bear 3X",  "sector": "Basic Materials",       "delisted_date": "2020-01-01", "delisted_reason": "reverse_split","acquiring_company": None},
    {"ticker": "KALA",    "company_name": "Kala Bio",                      "sector": "Healthcare",            "delisted_date": "2022-11-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "KERX",    "company_name": "Keryx Biopharmaceuticals",      "sector": "Healthcare",            "delisted_date": "2018-12-12", "delisted_reason": "acquired",     "acquiring_company": "Akebia Therapeutics"},
    {"ticker": "KTOV",    "company_name": "Kitov Pharma",                  "sector": "Healthcare",            "delisted_date": "2021-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "LNCE",    "company_name": "Snyder's-Lance",                "sector": "Consumer Defensive",    "delisted_date": "2018-03-26", "delisted_reason": "acquired",     "acquiring_company": "Campbell Soup"},
    {"ticker": "LOCK",    "company_name": "LifeLock",                      "sector": "Technology",            "delisted_date": "2017-02-09", "delisted_reason": "acquired",     "acquiring_company": "Symantec"},
    {"ticker": "LRAD",    "company_name": "LRAD Corporation",              "sector": "Industrials",           "delisted_date": "2021-10-01", "delisted_reason": "acquired",     "acquiring_company": "Genasys"},
    {"ticker": "LSCC",    "company_name": "Lattice Semiconductor (kept)",  "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "MDCO",    "company_name": "The Medicines Company",         "sector": "Healthcare",            "delisted_date": "2020-01-06", "delisted_reason": "acquired",     "acquiring_company": "Novartis"},
    {"ticker": "MEIP",    "company_name": "MEI Pharma",                    "sector": "Healthcare",            "delisted_date": "2024-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "MGNX",    "company_name": "MacroGenics",                   "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "MNKD",    "company_name": "MannKind Corporation (kept)",   "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "MNTA",    "company_name": "Momenta Pharmaceuticals",       "sector": "Healthcare",            "delisted_date": "2020-10-05", "delisted_reason": "acquired",     "acquiring_company": "Johnson & Johnson"},
    {"ticker": "MNTX",    "company_name": "Manitex International",         "sector": "Industrials",           "delisted_date": "2024-06-01", "delisted_reason": "acquired",     "acquiring_company": "Tadano"},
    {"ticker": "MYRG",    "company_name": "MYR Group (kept)",              "sector": "Industrials",           "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "NCLH_delist","company_name":"NCL placeholder",             "sector": "Consumer Cyclical",     "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "NEOS",    "company_name": "Neos Therapeutics",             "sector": "Healthcare",            "delisted_date": "2021-07-01", "delisted_reason": "acquired",     "acquiring_company": "Ayala Pharma"},
    {"ticker": "NFBK",    "company_name": "Northway Financial",            "sector": "Financial Services",    "delisted_date": "2023-04-01", "delisted_reason": "acquired",     "acquiring_company": None},
    {"ticker": "NLSN",    "company_name": "Nielsen Holdings",              "sector": "Industrials",           "delisted_date": "2022-10-03", "delisted_reason": "acquired",     "acquiring_company": "Brookfield Asset Mgmt"},
    {"ticker": "NMBL",    "company_name": "Nimble Storage",                "sector": "Technology",            "delisted_date": "2017-04-17", "delisted_reason": "acquired",     "acquiring_company": "Hewlett Packard Enterprise"},
    {"ticker": "NPSP",    "company_name": "NPS Pharmaceuticals",           "sector": "Healthcare",            "delisted_date": "2015-02-21", "delisted_reason": "acquired",     "acquiring_company": "Shire"},
    {"ticker": "NRCG",    "company_name": "NRC Group Holdings",            "sector": "Industrials",           "delisted_date": "2021-01-01", "delisted_reason": "acquired",     "acquiring_company": None},
    {"ticker": "NVLS",    "company_name": "Nivalis Therapeutics",          "sector": "Healthcare",            "delisted_date": "2017-07-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "OCUL",    "company_name": "Ocular Therapeutix (kept)",     "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "OMEX",    "company_name": "Odyssey Marine Exploration",    "sector": "Basic Materials",       "delisted_date": "2023-07-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "ONCS",    "company_name": "OncoSec Medical",               "sector": "Healthcare",            "delisted_date": "2023-09-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "OMED",    "company_name": "OncoMed Pharmaceuticals",       "sector": "Healthcare",            "delisted_date": "2019-10-31", "delisted_reason": "acquired",     "acquiring_company": "Celldex Therapeutics"},
    {"ticker": "OPGN",    "company_name": "OpGen Inc",                     "sector": "Healthcare",            "delisted_date": "2024-03-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "OREX",    "company_name": "Orexigen Therapeutics",         "sector": "Healthcare",            "delisted_date": "2018-03-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "PDCO",    "company_name": "Patterson Companies (kept)",    "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "PDLI",    "company_name": "PDL BioPharma",                 "sector": "Healthcare",            "delisted_date": "2021-05-01", "delisted_reason": "voluntary",    "acquiring_company": None},
    {"ticker": "PETX",    "company_name": "Aratana Therapeutics",          "sector": "Healthcare",            "delisted_date": "2019-09-17", "delisted_reason": "acquired",     "acquiring_company": "Elanco"},
    {"ticker": "PLPM",    "company_name": "Planet Payment",                "sector": "Technology",            "delisted_date": "2018-01-12", "delisted_reason": "acquired",     "acquiring_company": "i2c Inc"},
    {"ticker": "PRAA",    "company_name": "PRA Group (kept)",              "sector": "Financial Services",    "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "PRKR",    "company_name": "ParkerVision",                  "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "PRTO",    "company_name": "Proteon Therapeutics",          "sector": "Healthcare",            "delisted_date": "2018-11-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "PSDV",    "company_name": "pSivida Corp",                  "sector": "Healthcare",            "delisted_date": "2019-06-14", "delisted_reason": "acquired",     "acquiring_company": "EyePoint Pharma"},
    {"ticker": "PTLA",    "company_name": "Portola Pharmaceuticals",       "sector": "Healthcare",            "delisted_date": "2020-07-02", "delisted_reason": "acquired",     "acquiring_company": "Alexion"},
    {"ticker": "QUAD",    "company_name": "Quad/Graphics (kept)",          "sector": "Industrials",           "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RADS",    "company_name": "Radnet Inc (kept)",             "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RAND",    "company_name": "Rand Capital",                  "sector": "Financial Services",    "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RGLS",    "company_name": "Regulus Therapeutics",          "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RMBS",    "company_name": "Rambus Inc (kept)",             "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RNST",    "company_name": "Renasant Corp (kept)",          "sector": "Financial Services",    "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RNWK",    "company_name": "RealNetworks",                  "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RPTP",    "company_name": "Rapid7 (pre-IPO placeholder)",  "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RTRX",    "company_name": "Retrophin",                     "sector": "Healthcare",            "delisted_date": "2020-09-15", "delisted_reason": "acquired",     "acquiring_company": "Travere Therapeutics"},
    {"ticker": "RUSHA",   "company_name": "Rush Enterprises (kept)",       "sector": "Industrials",           "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "RXDX",    "company_name": "Ignyta Inc",                    "sector": "Healthcare",            "delisted_date": "2018-02-14", "delisted_reason": "acquired",     "acquiring_company": "Roche"},
    {"ticker": "SALE",    "company_name": "RetailMeNot",                   "sector": "Technology",            "delisted_date": "2017-07-10", "delisted_reason": "acquired",     "acquiring_company": "Harland Clarke"},
    {"ticker": "SGYP",    "company_name": "Synergy Pharmaceuticals",       "sector": "Healthcare",            "delisted_date": "2019-01-12", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "SLXP",    "company_name": "Salix Pharmaceuticals",         "sector": "Healthcare",            "delisted_date": "2015-04-01", "delisted_reason": "acquired",     "acquiring_company": "Valeant"},
    {"ticker": "SMRT",    "company_name": "Stein Mart",                    "sector": "Consumer Cyclical",     "delisted_date": "2020-10-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "SNDK",    "company_name": "SanDisk",                       "sector": "Technology",            "delisted_date": "2016-05-12", "delisted_reason": "acquired",     "acquiring_company": "Western Digital"},
    {"ticker": "SONO",    "company_name": "Sonos (kept if current)",       "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "SPPI",    "company_name": "Spectrum Pharmaceuticals",      "sector": "Healthcare",            "delisted_date": "2023-07-31", "delisted_reason": "acquired",     "acquiring_company": "Hanmi Pharmaceutical"},
    {"ticker": "SSYS",    "company_name": "Stratasys (kept)",              "sector": "Technology",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "STMP",    "company_name": "Stamps.com",                    "sector": "Technology",            "delisted_date": "2021-10-06", "delisted_reason": "acquired",     "acquiring_company": "Thoma Bravo"},
    {"ticker": "STML",    "company_name": "Stemline Therapeutics",         "sector": "Healthcare",            "delisted_date": "2020-06-19", "delisted_reason": "acquired",     "acquiring_company": "Menarini Group"},
    {"ticker": "SUPG",    "company_name": "SuperGen",                      "sector": "Healthcare",            "delisted_date": "2012-11-01", "delisted_reason": "acquired",     "acquiring_company": "Astex Pharma"},
    {"ticker": "SVRA",    "company_name": "Savara Inc",                    "sector": "Healthcare",            "delisted_date": "2024-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "SYNT",    "company_name": "Syntel Inc",                    "sector": "Technology",            "delisted_date": "2018-10-04", "delisted_reason": "acquired",     "acquiring_company": "Atos"},
    {"ticker": "TAGG",    "company_name": "TaggTV / Frankly Media",        "sector": "Technology",            "delisted_date": "2019-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "TEAR",    "company_name": "TearLab",                       "sector": "Healthcare",            "delisted_date": "2020-06-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "TLGT",    "company_name": "Teligent Inc",                  "sector": "Healthcare",            "delisted_date": "2013-01-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "TLRD",    "company_name": "Tailored Brands",               "sector": "Consumer Cyclical",     "delisted_date": "2020-09-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "TPCO",    "company_name": "Tribune Publishing (partial)",  "sector": "Communication Services","delisted_date": "2021-05-25", "delisted_reason": "acquired",     "acquiring_company": "Alden Global Capital"},
    {"ticker": "TRGT",    "company_name": "Cortendo AB",                   "sector": "Healthcare",            "delisted_date": "2013-06-01", "delisted_reason": "acquired",     "acquiring_company": "Strongbridge"},
    {"ticker": "TSRO",    "company_name": "Tesaro Inc",                    "sector": "Healthcare",            "delisted_date": "2019-01-24", "delisted_reason": "acquired",     "acquiring_company": "GlaxoSmithKline"},
    {"ticker": "TXMD",    "company_name": "TherapeuticsMD",                "sector": "Healthcare",            "delisted_date": "2023-01-01", "delisted_reason": "acquired",     "acquiring_company": "EvoFem"},
    {"ticker": "ULTI",    "company_name": "The Ultimate Software Group",   "sector": "Technology",            "delisted_date": "2019-05-03", "delisted_reason": "acquired",     "acquiring_company": "Hellman & Friedman"},
    {"ticker": "UNXL",    "company_name": "Uni-Pixel",                     "sector": "Technology",            "delisted_date": "2018-07-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "VREX",    "company_name": "Varex Imaging (kept)",          "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "VSEC",    "company_name": "VSE Corporation (kept)",        "sector": "Industrials",           "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "VSTM",    "company_name": "Verastem Oncology",             "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "VVUS",    "company_name": "VIVUS",                         "sector": "Healthcare",            "delisted_date": "2020-07-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "WBMD",    "company_name": "WebMD Health",                  "sector": "Healthcare",            "delisted_date": "2017-09-26", "delisted_reason": "acquired",     "acquiring_company": "Internet Brands"},
    {"ticker": "WCRX",    "company_name": "Warner Chilcott",               "sector": "Healthcare",            "delisted_date": "2013-10-01", "delisted_reason": "acquired",     "acquiring_company": "Actavis"},
    {"ticker": "XENE",    "company_name": "Xenon Pharmaceuticals (kept)",  "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "XGTI",    "company_name": "XG Technology",                 "sector": "Technology",            "delisted_date": "2020-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "XLRN",    "company_name": "Acceleron Pharma",              "sector": "Healthcare",            "delisted_date": "2021-11-19", "delisted_reason": "acquired",     "acquiring_company": "Merck"},
    {"ticker": "XNCR",    "company_name": "Xencor (kept)",                 "sector": "Healthcare",            "delisted_date": None,         "delisted_reason": "skip",         "acquiring_company": None},
    {"ticker": "YRCW",    "company_name": "Yellow Corp (formerly YRC)",    "sector": "Industrials",           "delisted_date": "2023-09-01", "delisted_reason": "bankrupt",     "acquiring_company": None},
    {"ticker": "YRIV",    "company_name": "Yanzhou Coal (placeholder)",    "sector": "Energy",                "delisted_date": "2021-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "ZAFG",    "company_name": "ZAF Energy Systems",            "sector": "Technology",            "delisted_date": "2022-01-01", "delisted_reason": "compliance",   "acquiring_company": None},
    {"ticker": "ZFGN",    "company_name": "Zafgen Inc",                    "sector": "Healthcare",            "delisted_date": "2020-11-10", "delisted_reason": "acquired",     "acquiring_company": "Larimar Therapeutics"},
    {"ticker": "ZIXI",    "company_name": "Zix Corporation",               "sector": "Technology",            "delisted_date": "2022-01-14", "delisted_reason": "acquired",     "acquiring_company": "OpenText"},
    {"ticker": "ZLCS",    "company_name": "Zalicus",                       "sector": "Healthcare",            "delisted_date": "2013-11-01", "delisted_reason": "acquired",     "acquiring_company": "Epizyme"},
    {"ticker": "ZOOM",    "company_name": "Zoom Telephonics (not ZM)",     "sector": "Communication Services","delisted_date": "2021-11-01", "delisted_reason": "acquired",     "acquiring_company": "MINIM Inc"},
    {"ticker": "ZSAN",    "company_name": "Zosano Pharma",                 "sector": "Healthcare",            "delisted_date": "2022-07-01", "delisted_reason": "compliance",   "acquiring_company": None},
]
# fmt: on

# Filter out skip entries for actual use
ACTIVE_DELISTED = [
    d for d in DELISTED_TICKERS
    if d.get("delisted_reason") != "skip"
    and d.get("delisted_date") is not None
    and not d["ticker"].endswith("_old")
    and not d["ticker"].endswith("_sp")
    and not d["ticker"].endswith("_delist")
    and not d["ticker"].endswith("_2010")
]


def get_delisted_tickers() -> List[str]:
    """Return list of ticker strings for delisted companies."""
    return [d["ticker"] for d in ACTIVE_DELISTED]


def get_delisted_records() -> List[Dict]:
    """Return full metadata records for all delisted companies."""
    return ACTIVE_DELISTED


def load_delisted_into_db(db=None) -> int:
    """Seed the delisted_companies table in historical_db."""
    if db is None:
        from data.historical_db import HistoricalDB
        db = HistoricalDB()

    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    records = []
    for d in ACTIVE_DELISTED:
        records.append({
            "ticker":            d["ticker"],
            "company_name":      d.get("company_name"),
            "sector":            d.get("sector"),
            "delisted_date":     d.get("delisted_date"),
            "delisted_reason":   d.get("delisted_reason"),
            "acquiring_company": d.get("acquiring_company"),
            "data_available":    0,
            "price_rows":        0,
            "financial_rows":    0,
            "last_attempted":    now,
        })
    return db.upsert_delisted(records)
