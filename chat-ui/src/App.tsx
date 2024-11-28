import React from 'react';
import '@chatui/core/es/styles/index.less';
// 引入组件
import Chat, { Bubble, useMessages } from '@chatui/core';
// 引入样式
import '@chatui/core/dist/index.css';
import './chatui-theme.css'
import axios from 'axios';

const App = () => {
  const { messages, appendMsg, setTyping } = useMessages([]);

  // 默认快捷短语，可选
  const defaultQuickReplies = [
    {
      name: '查询商务退返单',
      isNew: true,
      isHighlight: true,
    },
    {
      name: '查询页面访问数据',
      isNew: true,
    },
    {
      name: '查询sellout数据',
      isHighlight: true,
    },
    {
      name: '查询用户登录数据',
    },
  ];

  // 快捷短语回调，可根据 item 数据做出不同的操作，这里以发送文本消息为例
  function handleQuickReplyClick(item) {
    handleSend('text', item.name);
  }

  function handleSend(type, val) {
    if (type === 'text' && val.trim()) {
      appendMsg({
        type: 'text',
        content: { text: val },
        position: 'right',
      });

      setTyping(true);

      axios.post('http://127.0.0.1:5000/api/chat', {"message": val}).then(response =>{
        console.log(JSON.stringify(response))
        appendMsg({
          type: 'text',
          content: { text: response.data.response },
        });
      }).catch(error =>{
        console.error(error)
      })
    }
  }

  function renderMessageContent(msg) {
    const { content } = msg;
    return <Bubble content={content.text} />;
  }

  return (
      <Chat
          navbar={{ title: 'MYCP智能助手' }}
          messages={messages}
          renderMessageContent={renderMessageContent}
          quickReplies={defaultQuickReplies}
          onQuickReplyClick={handleQuickReplyClick}
          onSend={handleSend}
      />
  );
};

export default App;
