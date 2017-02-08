from enum import Enum

class SPAN_FILE_ROW_TYPES(Enum):
    TYPE_NULL = -1
    TYPE_0 = 0
    TYPE_81 = 1
    TYPE_82 = 2
    TYPE_B = 3
    TYPE_83 = 4

class SPAN_FILE_PRODUCT_TYPE_CODES:
    phy = 'PHY'
    fut =  'FUT'
    cmb = 'CMB'
    oop = 'OOP'
    oof = 'OOF'
    ooc = 'OOC'

class SPAN_FILE_CONTRACT_TYPE:
    call = 'C'
    put = 'P'
    future = 'F'

class SPAN_CONSTANTS:
    option_months = ['F','G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']


class CME_SPAN_TYPE_0:
    exchange_complex_start = 2
    exchange_complex_end = 8

    business_date_start = 8
    business_date_end = 16

    settlement_or_intraday_start = 16
    settlement_or_intraday_end = 17

    file_identifier_start = 17
    file_identifier_end = 19

    file_format_start = 35
    file_format_end = 37

class CME_SPAN_TYPE_B:
    row_exchange_start = 2
    row_exchange_end = 5

    underlying_start = 99
    underlying_end = 109

    commodity_product_code_start = 5
    commodity_product_code_end = 15

    product_type_start = 15
    product_type_end = 18

    future_contract_month_start = 18
    future_contract_month_end = 24

    future_contract_day_or_week_code_start = 24
    future_contract_day_or_week_code_end = 26

    option_contract_month_start = 27
    option_contract_month_end = 33

    option_contract_day_or_week_code_start = 33
    option_contract_day_or_week_code_end = 35

    expiration_start = 91
    expiration_end = 99

    time_to_expiration_start = 72
    time_to_expiration_end = 79

class CME_SPAN_TYPE_8:
    row_exchange_start = 2
    row_exchange_end = 5

    commodity_product_code_start = 5
    commodity_product_code_end = 15

    underlying_start = 15
    underlying_end = 25

    product_type_start = 25
    product_type_end = 28

    option_type_start = 28
    option_type_end = 29

    future_contract_month_start = 29
    future_contract_month_end = 35

    option_contract_month_start = 38
    option_contract_month_end = 44

    option_strike_price_start = 47
    option_strike_price_end = 54


class CME_SPAN_TYPE_8_1:
    settlement_price_start = 108
    settlement_price_end = 122

class CME_SPAN_TYPE_8_2:
    settlement_price_start = 110
    settlement_price_end = 117




