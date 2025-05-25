import os
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Pub/Sub エミュレータのホストを設定
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

project_id = "my-local-project"
topic_id = "my-topic"  # publisher.py と同じトピックID
subscription_id = "my-push-subscription" # 新しいPushサブスクリプションID
# PushエンドポイントのURL (push_endpoint_server.py で指定したURL)
push_endpoint = "http://localhost:8081/push" # ポート番号を合わせる

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(project_id, topic_id)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def create_push_subscription_if_not_exists():
    """Push型のサブスクリプションが存在しない場合に作成します。"""
    push_config = pubsub_v1.types.PushConfig(
        push_endpoint=push_endpoint,
        # attributes={"x-goog-version": "v1"} # 必要に応じて属性を追加
    )
    try:
        subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "push_config": push_config,
                "ack_deadline_seconds": 60, # Ackの期限 (秒)
            }
        )
        print(f"Pushサブスクリプション {subscription_path} を {topic_path} に作成しました。")
        print(f"Pushエンドポイント: {push_endpoint}")
    except AlreadyExists:
        print(f"Pushサブスクリプション {subscription_path} は既に存在します。")
        # 注意: 既存のサブスクリプションが異なる設定 (例: 異なるエンドポイント) を持つ場合、
        # 更新するか、一度削除して再作成する必要があります。
        # ここでは、存在すればOKとしています。
        # 更新する場合は subscriber.update_subscription(...) を使用します。
    except Exception as e:
        print(f"Pushサブスクリプション作成中にエラーが発生しました: {e}")
        raise

if __name__ == "__main__":
    print(f"トピック {topic_path} に対するPushサブスクリプションを作成または確認します...")
    # 事前に publisher.py などでトピックが作成されていることを前提とします。
    # もしトピックが存在しない可能性がある場合は、ここでトピック作成処理も追加してください。
    # from google.cloud import pubsub_v1 as pubsub_publisher
    # publisher_client = pubsub_publisher.PublisherClient()
    # try:
    #     publisher_client.create_topic(request={"name": topic_path})
    #     print(f"トピック {topic_path} を作成しました。")
    # except AlreadyExists:
    #     print(f"トピック {topic_path} は既に存在します。")

    create_push_subscription_if_not_exists()
    print("\nPushサブスクリプションの設定が完了しました。")
    print(f"1. {os.path.basename(__file__)} はこのまま終了します。")
    print(f"2. `python push_endpoint_server.py` を実行してPushサーバーを起動してください。")
    print(f"3. `python publisher.py` を実行してメッセージを送信すると、Pushサーバーのログにメッセージが表示されます。")