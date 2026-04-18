(function () {
  const imageObjects = Array.isArray(window.ALBUM_IMAGES) ? window.ALBUM_IMAGES : [];
  if (!imageObjects.length) return;

  const images = imageObjects.map(x => x.url);

  let index = 0;
  let timer = null;

  const mainImage = document.getElementById("mainImage");
  const counter = document.getElementById("counter");
  const playBtn = document.getElementById("playBtn");
  const intervalInput = document.getElementById("intervalInput");
  const prevBtn = document.getElementById("prevBtn");
  const nextBtn = document.getElementById("nextBtn");
  const fullscreenBtn = document.getElementById("fullscreenBtn");

  function updateCounter() {
    if (counter) {
      counter.textContent = `${index + 1} / ${images.length}`;
    }
  }

  function preloadNext() {
    const nextIdx = (index + 1) % images.length;
    const img = new Image();
    img.src = images[nextIdx];
  }

  function render() {
    if (!mainImage) return;
    mainImage.src = images[index];
    updateCounter();
    preloadNext();
  }

  function nextImage() {
    index = (index + 1) % images.length;
    render();
  }

  function prevImage() {
    index = (index - 1 + images.length) % images.length;
    render();
  }

  function getIntervalSeconds() {
    const raw = parseInt(intervalInput?.value || "3", 10);
    if (Number.isNaN(raw) || raw < 1) return 3;
    return raw;
  }

  function updatePlayButton() {
    if (playBtn) {
      playBtn.textContent = timer ? "자동넘김 정지" : "자동넘김 시작";
    }
  }

  function startAutoPlay() {
    stopAutoPlay();
    const seconds = getIntervalSeconds();
    timer = setInterval(nextImage, seconds * 1000);
    updatePlayButton();
  }

  function stopAutoPlay() {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
    updatePlayButton();
  }

  function toggleAutoPlay() {
    if (timer) stopAutoPlay();
    else startAutoPlay();
  }

  async function toggleFullscreen() {
    if (!document.fullscreenElement) {
      await document.documentElement.requestFullscreen();
    } else {
      await document.exitFullscreen();
    }
  }

  prevBtn?.addEventListener("click", prevImage);
  nextBtn?.addEventListener("click", nextImage);
  playBtn?.addEventListener("click", toggleAutoPlay);
  fullscreenBtn?.addEventListener("click", toggleFullscreen);

  intervalInput?.addEventListener("change", () => {
    if (timer) startAutoPlay();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "ArrowRight") {
      nextImage();
    } else if (e.key === "ArrowLeft") {
      prevImage();
    } else if (e.key === " ") {
      e.preventDefault();
      toggleAutoPlay();
    } else if (e.key === "f" || e.key === "F") {
      toggleFullscreen();
    }
  });

  mainImage?.addEventListener("click", nextImage);

  render();
})();
