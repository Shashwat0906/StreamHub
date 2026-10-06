import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { LiveProvider } from './api/live';
import { App } from './App';
import { ToastProvider } from './components/toast';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ToastProvider>
      <LiveProvider>
        <App />
      </LiveProvider>
    </ToastProvider>
  </StrictMode>,
);
