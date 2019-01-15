x = '\xe1\x00\x1a\x00\x00\x00\x00\x00\x00\x00n-555524123453541-zgvgbsafbhasn'
hex_list = ["0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F"]
def num2hex(num):
    return hex_list[num]
y = u""
for i in x:
    try:
        z = unicode(i)
        y += z
    except:
        i_num = ord(i)
        i_h, i_l = divmod(i_num, 16)
        y += u"\\x" + unicode(hex_list[i_h]) + unicode(hex_list[i_l])
print(y, type(y))



