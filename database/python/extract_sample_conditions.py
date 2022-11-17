import re
from enum import Enum


class ExtendedCondition(Enum):
    H2O_NEGATIVE_CONTROL = 0
    EMPTY_WELL_NEGATIVE_CONTROL = 1
    NEGATIVE_PCR_TEST = 2
    NEGATIVE_CONTROL_FGCZ = 3
    TWIST_POSITIVE_CONTROL = 4
    POSITIVE_CONTROLS = 5
    ETHZ_ID_SAMPLE = 6
    WASTEWATER_SAMPLE = 7
    FGCZ_SAMPLE = 8
    BASEL_UZH_SEQENCING = 9
    LAB_DR_RISCH = 10
    USZ_TIER_SAMPLE = 11
    UNASSIGNED = -1
    NO_UNIQUE_ASSIGNMENT = -2

    def __str__(self):
        return super().__str__().split(".")[1]


class Condition(Enum):

    NEGATIVE_CONTROL = 0
    POSITIVE_CONTROL = 1
    EXPERIMENTAL_CONDITION = 2
    UNASSIGNED = -1
    NO_UNIQUE_ASSIGNMENT = -2

    def __str__(self):
        return super().__str__().split(".")[1]


_EC = ExtendedCondition  # for internal abbreviation

_re_mapping = {
    _EC.H2O_NEGATIVE_CONTROL: re.compile("^H2O"),  #  eg: H2O_CP002_A7
    _EC.EMPTY_WELL_NEGATIVE_CONTROL: re.compile(
        "^((EMPTY)|(empty))"
    ),  #  eg: EMPTY_CP002_A11
    _EC.NEGATIVE_PCR_TEST: re.compile("^neg_"),  # eg: neg_109_B2
    _EC.NEGATIVE_CONTROL_FGCZ: re.compile("^NTC_NA_NTC_NA"),  # eg: NTC_NA_NTC_NA
    _EC.TWIST_POSITIVE_CONTROL: re.compile(
        "^(pos_)|(Twist_control)"
    ),  # eg: pos_MN908947_3_1_100000_CP0002
    _EC.POSITIVE_CONTROLS: re.compile("CoV_ctrl_"),  # eg: CoV_ctrl_1_1_10000
    _EC.ETHZ_ID_SAMPLE: re.compile(
        "^[0-9]{6}(_Plate){0,1}_(p){0,1}[0-9]+"
    ),  # eg: 160000_434_D02
    _EC.WASTEWATER_SAMPLE: re.compile("^[0-9]{2}_202[0-9]_"),  # eg: 09_2020_03_24_B
    _EC.FGCZ_SAMPLE: re.compile(
        "^[0-9]{8}_Plate_[0-9]+"
    ),  # eg: 30430668_Plate_8_041120tb3_D7
    _EC.BASEL_UZH_SEQENCING: re.compile("^[A-Z][0-9]_[0-9]+"),  # eg: A2_722
    _EC.LAB_DR_RISCH: re.compile(
        "^674597001"
    ),  # lone sample by the lab "Labormedizinisches Zentrum Dr Risch": 674597001
    _EC.USZ_TIER_SAMPLE: re.compile("^USZ_[0-9]_Tier"),  # eg: USZ_5_Tier
}


_condition_grouping = {
    _EC.H2O_NEGATIVE_CONTROL: Condition.NEGATIVE_CONTROL,
    _EC.EMPTY_WELL_NEGATIVE_CONTROL: Condition.NEGATIVE_CONTROL,
    _EC.NEGATIVE_PCR_TEST: Condition.NEGATIVE_CONTROL,
    _EC.NEGATIVE_CONTROL_FGCZ: Condition.NEGATIVE_CONTROL,
    _EC.TWIST_POSITIVE_CONTROL: Condition.POSITIVE_CONTROL,
    _EC.POSITIVE_CONTROLS: Condition.POSITIVE_CONTROL,
    _EC.ETHZ_ID_SAMPLE: Condition.EXPERIMENTAL_CONDITION,
    _EC.WASTEWATER_SAMPLE: Condition.EXPERIMENTAL_CONDITION,
    _EC.FGCZ_SAMPLE: Condition.EXPERIMENTAL_CONDITION,
    _EC.BASEL_UZH_SEQENCING: Condition.EXPERIMENTAL_CONDITION,
    _EC.LAB_DR_RISCH: Condition.EXPERIMENTAL_CONDITION,
    _EC.USZ_TIER_SAMPLE: Condition.EXPERIMENTAL_CONDITION,
    _EC.UNASSIGNED: Condition.UNASSIGNED,
    _EC.NO_UNIQUE_ASSIGNMENT: Condition.NO_UNIQUE_ASSIGNMENT,
}


def extract_sample_condition(sample):
    """Function to parse a sample name and return its condition."""

    matches = [
        condition for (condition, regex) in _re_mapping.items() if regex.search(sample)
    ]

    if not matches:
        ec = _EC.UNASSIGNED

    elif len(matches) > 1:
        ec = _EC.NO_UNIQUE_ASSIGNMENT

    else:
        ec = matches[0]

    return _condition_grouping[ec], ec


if __name__ == "__main__":
    import sys
    names = sys.argv[1:]
    for name in names:
        print(str(extract_sample_condition(name)[1]))
