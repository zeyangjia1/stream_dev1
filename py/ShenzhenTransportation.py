import requests
import pandas as pd
import time
from tqdm import tqdm

def fetch_sz_traffic_data(app_key, max_pages=20):
    """获取深圳市交通数据
    Args:
        app_key: API访问密钥
        max_pages: 最大获取页数
    Returns:
        pandas DataFrame包含交通数据
    """
    # API基础URL
    base_url = "https://opendata.sz.gov.cn/api/29200_00403627/1/service.xhtml"
    # 存储所有页面的数据
    all_data = []
    # 设置请求头，模拟浏览器访问
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36"
    }

    print("开始获取深圳市交通数据...")
    # 使用tqdm显示进度条
    for page in tqdm(range(1, max_pages + 1)):
        # 构建请求参数
        params = {
            "appKey": app_key,
            "page": page,
            "rows": 100  # 每页100条记录
        }

        try:
            # 发送POST请求获取数据
            response = requests.post(base_url, data=params, headers=headers, timeout=30)
            # 检查请求是否成功
            response.raise_for_status()
            # 解析JSON响应
            data = response.json()

            # 动态获取总页数
            total_records = data.get('total', 0)
            total_pages = (total_records + 99) // 100  # 向上取整计算总页数
            max_pages = min(max_pages, total_pages)  # 取较小值避免无效请求

            # 检查当前页是否有数据
            if not data.get('data'):
                print(f"第 {page} 页无数据，停止获取")
                break

            # 将当前页数据添加到总数据列表
            all_data.extend(data['data'])
            time.sleep(0.5)  # 延时避免触发API频率限制

        except requests.exceptions.HTTPError as e:
            # 处理HTTP错误
            print(f"HTTP错误 {e.response.status_code}，停止获取")
            break
        except Exception as e:
            # 处理其他异常
            print(f"获取第 {page} 页失败: {str(e)}")
            break

    # 检查是否获取到数据
    if not all_data:
        print("未获取到任何数据，请检查API密钥或网络连接")
        return pd.DataFrame()  # 返回空DataFrame

    print(f"共获取 {len(all_data)} 条记录")
    return pd.DataFrame(all_data)  # 转换为DataFrame返回

if __name__ == "__main__":
    # API密钥，需要替换为实际密钥
    APP_KEY = "7b972d41a45b45a5bdd79faff272a75d"  # 替换为实际的appKey
    # 调用函数获取数据
    df = fetch_sz_traffic_data(APP_KEY)

    if not df.empty:
        # 打印数据基本信息
        print("\n数据基本信息:")
        df.info()

        # 打印数据包含的列
        print("\n数据包含的列:")
        print(df.columns.tolist())

        # 定义字段映射，根据实际数据调整
        field_mapping = {
            'time_field': 'START_TIME',  # 使用START_TIME作为时间字段
            'direction_field': None      # 数据中没有方向字段
        }

        # 检查时间字段是否存在
        if field_mapping['time_field'] not in df.columns:
            print(f"错误: 数据缺少时间字段: {field_mapping['time_field']}")
            print("可用字段:", df.columns.tolist())
        else:
            # 处理时间字段，转换为datetime类型
            time_field = field_mapping['time_field']
            try:
                df[time_field] = pd.to_datetime(df[time_field])
                print(f"已转换时间字段: {time_field}")
            except Exception as e:
                print(f"时间字段转换失败: {str(e)}")

            # 保存数据到CSV文件
            csv_file = "shenzhen_traffic_data.csv"
            df.to_csv(csv_file, index=False, encoding='utf_8_sig')  # 使用UTF-8带BOM编码
            print(f"数据已保存至: {csv_file}")

            # 打印数据样例
            print("\n数据样例:")
            print(df.head().to_dict(orient='records'))
    else:
        print("未获取到有效数据")