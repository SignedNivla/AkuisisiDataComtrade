# from country_converter import CountryConverter
# from hdx.location.country import Country
# import time

# coco = CountryConverter()

# test = coco.convert('CHN',src='ISO3',to='UNCode')
# print(test)

# a = [str(i) for i in range(2020,2025)]

# b = ", ".join(a)

# print(type(b))

year_str = "2005"
for i in range(1,13):
    month_str = f"{i:02d}"
    period_str = f"{year_str}{month_str}"

    print(period_str)