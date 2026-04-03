
with open('config/settings.yaml', 'r') as f:

    content = f.read()

content = content.replace("bot_token: ''", "bot_token: '8789592090: AAFM1TJkw1nZz5J9ndiPHs xvbbFf8U0-VVU'")

content = content.replace("chat_id: ''", "chat_id: '8508697534'")

content = content.replace('enabled: false', 'enabled: true')

with open('config/settings.yaml', 'w') as f:

    f.write(content)

print('Done!')

