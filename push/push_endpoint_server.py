from flask import Flask, request, Response
import base64
import json
import os

app = Flask(__name__)

# Pub/Subからのリクエストの正当性を検証するためのトークン (エミュレータでは通常不要だが、参考として)
# PUSH_VERIFICATION_TOKEN = os.environ.get("PUSH_VERIFICATION_TOKEN", "your-secret-token")

@app.route('/push', methods=['POST'])
def push_handler():
    # # 簡単なトークン検証 (本番環境ではより堅牢な認証を推奨)
    # auth_token = request.args.get('token', None)
    # if PUSH_VERIFICATION_TOKEN and auth_token != PUSH_VERIFICATION_TOKEN:
    #     print(f"Invalid token: {auth_token}")
    #     return Response(status=403) # Forbidden

    try:
        envelope = json.loads(request.data.decode('utf-8'))
        print("\n--- メッセージ受信 ---")
        print(f"Raw envelope: {json.dumps(envelope, indent=2)}") # デバッグ用に完全なエンベロープを表示

        message_data = envelope.get('message', {})
        if not message_data:
            print("Empty message received.")
            return Response(status=400) # Bad Request

        # メッセージデータはBase64エンコードされている
        payload_bytes = base64.b64decode(message_data.get('data', ''))
        payload = payload_bytes.decode('utf-8')
        message_id = message_data.get('messageId')
        publish_time = message_data.get('publishTime')
        attributes = message_data.get('attributes')

        print(f"  ID: {message_id}")
        print(f"  公開日時: {publish_time}")
        print(f"  データ: {payload}")
        if attributes:
            print(f"  属性: {attributes}")

        # ここでメッセージに対する処理を行います (例: データベースへの保存など)

        # Pub/Sub に成功を通知 (2xxのステータスコード)
        # 200, 201, 202, 204, 102 など
        return Response(status=204) # No Content が一般的
    except Exception as e:
        print(f"エラー処理中: {e}")
        # エラーが発生した場合、Pub/Subはメッセージを再送します
        # (適切なエラーコードを返すことで制御可能)
        return Response(status=500) # Internal Server Error

if __name__ == '__main__':
    print("Pushエンドポイントサーバーを http://localhost:8081/push で起動します...")
    # ポート番号は他のサービスと競合しないものを選んでください
    app.run(port=8081, debug=False) # debug=Trueにすると開発時に便利ですが、本番同様のテストではFalse推奨