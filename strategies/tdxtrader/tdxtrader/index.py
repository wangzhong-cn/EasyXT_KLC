import time

from tdxtrader.anis import RED, RESET
from tdxtrader.file import clear_file_content
from tdxtrader.logger import add_wechat_handler, logger
from tdxtrader.order import cancel_order, create_order
from tdxtrader.trader import create_trader


def start(account_id, mini_qmt_path, file_path, buy_sign, sell_sign, buy_event, sell_event, interval=1, cancel_after=None, wechat_webhook_url=None, block_files=None):

    add_wechat_handler(logger, wechat_webhook_url)

    xt_trader, account = create_trader(account_id, mini_qmt_path)

    previous_df = None

    # 启动前清空文件内容
    clear_file_content(file_path)

    while True:
        try:
            previous_df = create_order(xt_trader, account, file_path, previous_df, buy_sign, sell_sign, buy_event, sell_event, block_files)
            # 撤单
            cancel_order(xt_trader, account, cancel_after)

        except Exception as e:
            logger.error(f"{RED}【程序错误】{RESET}{e}")

        time.sleep(interval)
