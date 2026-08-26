import pandas as pd


if __name__ == "__main__":

    base_path = r"W:\project\python_project\oke_auto_trade\kline_data"

    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "LINKUSDT",
        "AAVEUSDT",
        "BNBUSDT"
    ]

    use_cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    for symbol in symbols:

        input_file = rf"{base_path}\{symbol}_1s_2021-01-01_merged.csv"
        output_file = input_file.replace(".csv", "_6cols.csv")

        print(f"正在处理: {symbol}")

        # 只读取前6列
        df = pd.read_csv(
            input_file,
            usecols=use_cols
        )

        # 保存新文件
        df.to_csv(
            output_file,
            index=False
        )

        print("保存完成:", output_file)

    print("全部币种处理完成！")