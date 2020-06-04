import requests
import time
import random
from mysql_model import Mysql
from langconv import *
from lxml import etree
import schedule


# proxy = {
#     "http": "http://test816w:test816w@45.125.33.22:888/"
# }


headers = {
   #  'Accept': '*/*',
   #  'Accept-Encoding': 'gzip, deflate, br',
   #  'Accept-Language': 'zh-CN,zh;q=0.9',
   #  'Connection': 'keep-alive',
   # ' Content-Length': '85',
   #  'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',

    "Host": "www.sfc.hk",
    "Origin": "https://www.sfc.hk",
    "Referer": "https://www.sfc.hk/publicregWeb/searchByRa?locale=zh",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",

}

detail_url_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'
}

def get_ip():
    """
    代理函数 已弃用
    :return:
    """
    ip_pool = [False, {"http": "http://test816w:test816w@45.125.33.22:888/"}, {"http": "http://{}".format(requests.get(url="http://localhost:5555/random").text)}]
    return random.choice(ip_pool)

def get_data(ratype, nameStartLetter, page_num, start_num, table):
    """
    采集主函数
    :param ratype:
    :param nameStartLetter:
    :param page_num:
    :param start_num:
    :param table:
    :return:
    """
    global all_ceref

    dc = int(round(time.time() * 1000))

    data = {
        "licstatus": "active",
        "ratype": ratype,
        "roleType": "corporation",
        "nameStartLetter": nameStartLetter,
        "page": page_num,
        "start": start_num,
        "limit": "20",
    }
    url = "https://www.sfc.hk/publicregWeb/searchByRaJson?_dc={}".format(dc)
    i = 0
    while True:
        try:
            print("开始请求 牌照{} 条件{} 起始条目{} {}条".format(ratype, nameStartLetter, start_num, 20))
            time.sleep(random.uniform(0, 2))
            rep = requests.post(url, data=data, headers=detail_url_headers, timeout=20)

            if rep.status_code == 200:
                text = rep.text
                cerefs = re.findall(r'"ceref":"(.*?)","', text)
                nums = re.findall(r'"totalCount":(.*?),', text)

                if nums[0] == "0":
                    print("{}-相关资料为空,进入下一个条件")
                    return False
                if cerefs == []:
                    print("{}-此条件已采集完成，进入下一个条件")
                    return False

                else:
                    long_time = len(cerefs)
                    for ceref in cerefs:
                        # ceref = "AAF564"
                        while True:
                            time.sleep(random.uniform(0, 2))
                            addresses_url = "https://www.sfc.hk/publicregWeb/corp/{}/addresses".format(ceref)
                            try:
                                print("开始请求{}".format(addresses_url))

                                rep = requests.get(addresses_url, headers=detail_url_headers, timeout=20)

                                if rep.status_code == 200:
                                    text = rep.text
                                    sel = etree.HTML(text)
                                    company_name = sel.xpath('//*[@id="layoutDiv"]/div[2]/div[3]/p/text()')
                                    if company_name:
                                        company_name = "".join(company_name).replace("\r\n", "").replace(" ", "").replace("-", "").replace(":", "")
                                    else:
                                        company_name = None
                                    Email = re.findall(r'"email":"(.*?)"', text)
                                    if Email:
                                        Email = "\n".join(Email)
                                    else:
                                        Email = None

                                    Website = re.findall(r'"website":"(.*?)"', text)
                                    if Website:
                                        Website = "\n".join(Website)
                                    else:
                                        Website = None

                                    address = re.findall(r'"fullAddressChin":"(.*?)"', text)
                                    if address:
                                        address = "\n".join(address)
                                    else:
                                        address = None
                                    break


                            except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, Exception) as e:
                                print("{}失败。重试 原因{}".format(addresses_url, e))
                                continue

                        while True:

                            co_url = "https://www.sfc.hk/publicregWeb/corp/{}/co".format(ceref)
                            try:
                                print("开始请求{}".format(co_url))
                                time.sleep(random.uniform(0, 2))
                                rep = requests.get(co_url, headers=detail_url_headers, timeout=20)

                                if rep.status_code == 200:
                                    text = rep.text
                                    phone = re.findall(r'"tel":"(.*?)"', text)
                                    if phone:
                                        phone = "\n".join(phone)
                                    else:
                                        phone = None
                                    fax = re.findall(r'"fax":"(.*?)"', text)
                                    if fax:
                                        fax = "\n".join(fax)
                                    else:
                                        fax = None
                                    complaint_email = re.findall(r'"email":"(.*?)"', text)
                                    if complaint_email:
                                        complaint_email = "\n".join(complaint_email)
                                    else:
                                        complaint_email = None
                                    postal_address = re.findall(r'"fullAddressChin":"(.*?)"', text)
                                    if postal_address:
                                        postal_address = "\n".join(postal_address)
                                    else:
                                        postal_address = None
                                    break

                            except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, Exception) as e:
                                print("{}失败。重试 原因{}".format(co_url, e))
                                continue

                        while True:

                            detail_url = "https://www.sfc.hk/publicregWeb/corp/{}/details".format(ceref)
                            try:
                                print("开始请求{}".format(detail_url))
                                time.sleep(random.uniform(0, 2))
                                rep = requests.get(detail_url, headers=detail_url_headers, timeout=20)

                                if rep.status_code == 200:
                                    text = rep.text
                                    licenses = re.findall("var raDetailData = (.*?);", text)   # 牌照信息
                                    if licenses:
                                        licenses = eval(licenses[0].replace("null", '""'))
                                        licenses = "\n".join([i["cactDesc"] + ' ' + i["effDate"]for i in licenses])   # 拼接为字符串
                                    else:
                                        licenses = None
                                    break


                            except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, Exception) as e:
                                print("{}失败。重试 原因{}".format(detail_url, e))
                                continue

                        while True:
                            ro_url = "https://www.sfc.hk/publicregWeb/corp/{}/ro".format(ceref)
                            try:
                                print("开始请求{}".format(ro_url))
                                time.sleep(random.uniform(0, 2))
                                rep = requests.get(ro_url, headers=detail_url_headers, timeout=20)

                                if rep.status_code == 200:
                                    text = rep.text
                                    OAs = re.findall("var roData = (.*?);", text)   # OA信息
                                    if OAs:
                                        OAs = eval(OAs[0].replace("null", '""'))
                                        OAs = "\n".join([i["ceRef"] + ' ' + i["fullName"] + i["entityNameChi"] + ' ' + " ".join(["RA{}".format(j["actType"]) for j in i["regulatedActivities"]]) for i in OAs])
                                    else:
                                        OAs = None
                                    break

                            except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, Exception) as e:
                                print("{}失败。重试 原因{}".format(ro_url, e))
                                continue

                        while True:
                            rep_url = "https://www.sfc.hk/publicregWeb/corp/{}/rep".format(ceref)
                            try:
                                print("开始请求{}".format(rep_url))
                                time.sleep(random.uniform(0, 2))
                                rep = requests.get(rep_url, headers=detail_url_headers, timeout=20)

                                if rep.status_code == 200:
                                    text = rep.text
                                    reps = re.findall("var repData = (.*?);", text)   # OA信息
                                    if reps:
                                        reps = eval(reps[0].replace("null", '""'))
                                        reps = "\n".join([i["ceRef"] + ' ' + i["fullName"] + i["entityNameChi"] + ' ' + " ".join(["RA" + str(j["actType"]) for j in i["regulatedActivities"]]) for i in reps])
                                    else:
                                        reps = None
                                    break

                            except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, Exception) as e:
                                print("{}失败。重试 原因{}".format(rep_url, e))
                                continue


                        items = [{
                            "Ceref": ceref,
                            "Company_name": company_name,
                            "Chi_Company_name": Traditional2Simplified(company_name),  # 公司名称简体
                            "Address": address,
                            "Postal_Address": postal_address,
                            "Email": Email,
                            "Complaint_Email": complaint_email,
                            "Website": Website,
                            "Phone": phone,
                            "Fax": fax,
                            "Licenses": licenses,
                            "OAs": OAs,
                            "reps": reps
                            }]
                        Mysql.save_data(items, ceref, table)

                break
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, Exception) as e:
            print("{}失败 重试 错误{}".format(url, e))
            i = i + 1
            continue

def Traditional2Simplified(sentence):
    '''
    将sentence中的繁体字转为简体字
    :param sentence: 待转换的句子
    :return: 将句子中繁体字转换为简体字之后的句子
    '''
    sentence = Converter('zh-hans').convert(sentence)
    return sentence


def HK_license_spider(license_num_ls):
    """
    港交所 金融牌照信息 启动函数
    :param license_num_ls: 需要采集的牌照 ['1', '9'...]
    :return:
    """
    Mysql.delete_data('hk_financial_licence')
    print("数据消除完毕 等待更新======")

    ls = [i.upper() for i in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']] + [i for i in range(10)]
    for j in license_num_ls:
        for i in ls:
            start_num = 0
            for page_num in range(1, 100):
                flag = get_data(j, i, str(page_num), str(start_num), 'hk_financial_licence')
                if flag == False:
                    break
                start_num = start_num + 20



# schedule.every().friday.at("05:30").do(HK_license_spider, ["1", "9"])
# while True:
#     schedule.run_pending()  # 运行程序

HK_license_spider(["9", "1", "2", "3", "4", "5", "6", "7", "8", "10"])



