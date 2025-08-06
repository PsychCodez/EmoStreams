const axios = require('axios');

// Function to generate random emoji from a predefined list
const getRandomEmoji = () => {
  const emojis = [
    '😀', '😁', '😂', '🤣', '😃', '😄', '😅', '😆', '😉', '😊',
    '😋', '😎', '😍', '🥰', '😘', '😗', '😙', '😚', '🙂', '🤗',
    '🤩', '🥳', '😛', '😜', '😝', '😎', '🤓', '🤔', '😒', '🙄'
  ];
  return emojis[Math.floor(Math.random() * emojis.length)];
};

const sendEmojiData = async (user_id) => {
  try {
    for (let i = 1; i > 0; i++) {
      const random_emoji = getRandomEmoji();
      const generated_time = new Date().toISOString(); // Get current timestamp

      const message = {
        user_id: user_id,
        emoji_type: random_emoji,
        timestamp: generated_time
      };

      // Send the data to the server
      await axios.post('http://localhost:3000/send-emoji', message);
      console.log('Client: Sent emoji #${i + 1}');
    }
  } catch (error) {
    console.error('Error sending emoji data:', error);
  }
};

// Run the function to send emoji data
const user_id = 'user2'; // Unique user ID for this client
sendEmojiData(user_id);
