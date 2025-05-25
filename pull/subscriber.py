import os
import time
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Pub/Sub エミュレータのホストを設定
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

project_id = "my-local-project"
topic_id = "my-topic" # publisher.py と同じトピックID
subscription_id = "my-subscription"

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(project_id, topic_id) # PublisherClientでも可
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def create_subscription_if_not_exists(subscriber_client, topic_path_for_sub, subscription_path_to_create):
    """サブスクリプションが存在しない場合に作成します。"""
    try:
        subscriber_client.create_subscription(
            request={"name": subscription_path_to_create, "topic": topic_path_for_sub}
        )
        print(f"サブスクリプション {subscription_path_to_create} を {topic_path_for_sub} に作成しました。")
    except AlreadyExists:
        print(f"サブスクリプション {subscription_path_to_create} は既に存在します。")
    except Exception as e:
        print(f"サブスクリプション作成中にエラーが発生しました: {e}")
        raise

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """受信したメッセージを処理するコールバック関数。"""
    print(f"受信しました: {message.data.decode()}")
    message.ack() # メッセージの受信を確認

def receive_messages():
    """サブスクリプションからメッセージを受信します。"""
    create_subscription_if_not_exists(subscriber, topic_path, subscription_path)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"{subscription_path} でメッセージを待機しています...")

    # タイムアウト（秒）を設定して、プログラムが無限に実行されないようにする
    # Ctrl+C で停止することも可能
    try:
        # streaming_pull_future.result(timeout=60) # 60秒後にタイムアウト
        streaming_pull_future.result() # タイムアウトなし（手動で停止するまで実行）
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result() # キャンセルを完了させる
        print("メッセージの待機をタイムアウトしました。")
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result() # キャンセルを完了させる
        print("サブスクライバーを停止しました。")
    except Exception as e:
        print(f"メッセージ受信中に予期せぬエラーが発生しました: {e}")
        streaming_pull_future.cancel()
        streaming_pull_future.result()

if __name__ == "__main__":
    receive_messages()