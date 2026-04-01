# 실제투자로 진행할 시 True를 False로 변경
is_paper_trading = False

# 따옴표 안에 작성할 것
real_app_key = "xSDKU4WFAtEFKiKqubSxFtb0UzrL_HHj-eIOJaw7auI"
real_app_secret = "o99uGbk4vEszFTYFdjLig4DyNytpnAoXvcnoIFiAmKQ"

paper_app_key = "rpEu17fvQIEIET1aRLRaqC9xTcpl8o09GE6NNvrlmT0"
paper_app_secret = "QybqEnbtBGUSgbjK-26O1QaqTvary1luACNOjv13YH8"

real_host_url = "https://api.kiwoom.com"
paper_host_url = "https://mockapi.kiwoom.com"

real_socket_url = "wss://api.kiwoom.com:10000"
paper_socket_url = "wss://mockapi.kiwoom.com:10000"

app_key = paper_app_key if is_paper_trading else real_app_key
app_secret = paper_app_secret if is_paper_trading else real_app_secret
host_url = paper_host_url if is_paper_trading else real_host_url
socket_url = paper_socket_url if is_paper_trading else real_socket_url

telegram_chat_id = "6147437980"
telegram_token = "8772810107:AAFqJ5aNzyEgLOzx7FemlAXnAs7T2orkO-k"	