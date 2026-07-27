from os import getenv
from dotenv import load_dotenv

load_dotenv()

SERVER_ADDRESS = getenv("SERVER_ADDRESS")
PORT_NUMBER = getenv("PORT_NUMBER")

ID_DVB_C_ANALYZER_CRICKET = int(getenv("ID_DVB_C_ANALYZER_CRICKET", 10))
ID_IPTV_ANALYZER_G2X = int(getenv("ID_IPTV_ANALYZER_G2X", 9))
ID_IPTV_ANALYZER_SURVEYOR = int(getenv("ID_IPTV_ANALYZER_SURVEYOR", 0))

# если нет в переменных окружения, по умолчанию +

