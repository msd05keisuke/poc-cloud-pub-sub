import os
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Pub/Sub エミュレータのホストを設定
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

project_id = "my-local-project"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def create_topic_if_not_exists(publisher_client, topic_path_to_create):
    """トピックが存在しない場合に作成します。"""
    try:
        publisher_client.create_topic(request={"name": topic_path_to_create})
        print(f"トピック {topic_path_to_create} を作成しました。")
    except AlreadyExists:
        print(f"トピック {topic_path_to_create} は既に存在します。")
    except Exception as e:
        print(f"トピック作成中にエラーが発生しました: {e}")
        raise

def publish_messages():
    """トピックにメッセージを送信します。"""
    create_topic_if_not_exists(publisher, topic_path)

    for i in range(5):
        message_data = f"メッセージ {i+1}".encode("utf-8")
        try:
            future = publisher.publish(topic_path, message_data)
            print(f"Publish しました (ID: {future.result()}): {message_data.decode()}")
        except Exception as e:
            print(f"メッセージのPublish中にエラーが発生しました: {e}")
            # 必要に応じてエラー処理を追加

if __name__ == "__main__":
    publish_messages()