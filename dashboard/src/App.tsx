import { BrowserRouter, Routes, Route } from 'react-router-dom';

import { JobList } from './pages/JobList';
import { JobDetail } from './pages/JobDetail';
import './App.css';

function App() {
  return (
    <BrowserRouter>
      <main>
        <Routes>
          <Route path="/" element={<JobList />} />
          <Route path="/jobs/:jobId" element={<JobDetail />} />
        </Routes>
      </main>
    </BrowserRouter>
  );
}

export default App;
