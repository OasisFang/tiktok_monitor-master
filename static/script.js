function showCharacter() {
    const character = document.getElementById('character');
    character.style.display = 'block';
    moveCharacter();
    setTimeout(() => {
        character.style.display = 'none';
    }, 1000);
}

function moveCharacter() {
    const character = document.getElementById('character');
    const x = Math.random() * (window.innerWidth - 50);
    const y = Math.random() * (window.innerHeight - 50);
    character.style.transform = `translate(${x}px, ${y}px)`;
}

async function fetchGiftInfo() {
    const response = await fetch('/get_gift_info');
    const data = await response.json();
    if (data.length > 0) {
        showCharacter();
    }
}

setInterval(fetchGiftInfo, 3000); // 每3秒检查一次是否有新的礼物
