(() => {
    const root = document.documentElement;
  
    // Theme toggle (remembers choice)
    const themeBtn = document.getElementById('theme');
    themeBtn?.addEventListener('click', () => {
      const next = root.dataset.theme === 'light' ? 'dark' : 'light';
      root.dataset.theme = next;
      try { localStorage.setItem('theme', next); } catch (e) {}
    });
  
    // Mobile menu
    const burger = document.getElementById('burger');
    const menu = document.getElementById('menu');
    const setMenu = (open) => {
      burger.setAttribute('aria-expanded', String(open));
      burger.setAttribute('aria-label', open ? 'Close menu' : 'Open menu');
      menu.classList.toggle('open', open);
    };
    burger?.addEventListener('click', () => setMenu(burger.getAttribute('aria-expanded') !== 'true'));
    menu?.querySelectorAll('a').forEach(a => a.addEventListener('click', () => setMenu(false)));
    document.addEventListener('keydown', e => { if (e.key === 'Escape') setMenu(false); });
  
    // Nav border on scroll
    const nav = document.querySelector('.nav');
    const onScroll = () => nav.classList.toggle('scrolled', window.scrollY > 8);
    onScroll();
    window.addEventListener('scroll', onScroll, { passive: true });
  
    // Scroll reveal
    const items = document.querySelectorAll('.reveal');
    if (!('IntersectionObserver' in window)) {
      items.forEach(el => el.classList.add('in'));
    } else {
      const io = new IntersectionObserver(entries => {
        entries.forEach(entry => {
          if (entry.isIntersecting) { entry.target.classList.add('in'); io.unobserve(entry.target); }
        });
      }, { threshold: 0.12, rootMargin: '0px 0px -40px 0px' });
      items.forEach(el => io.observe(el));
    }
  
    // Footer year
    const y = document.getElementById('year');
    if (y) y.textContent = new Date().getFullYear();
  })();