import { useEffect, useState } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

// ---------------------------------------------------------------------------
// API Response Types
// ---------------------------------------------------------------------------

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRateEntry {
  task: string
  avg_score: number
  attempts: number
}

interface Lab {
  id: string
  title: string
}

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

// ---------------------------------------------------------------------------
// Fetch Helpers
// ---------------------------------------------------------------------------

const STORAGE_KEY = 'api_key'

function getApiKey(): string {
  return localStorage.getItem(STORAGE_KEY) ?? ''
}

async function fetchWithAuth<T>(url: string): Promise<T> {
  const token = getApiKey()
  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  })
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`)
  }
  return res.json() as Promise<T>
}

// ---------------------------------------------------------------------------
// Dashboard Component
// ---------------------------------------------------------------------------

export default function Dashboard() {
  const [labs, setLabs] = useState<Lab[]>([])
  const [selectedLab, setSelectedLab] = useState<string>('')
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineEntry[]>([])
  const [passRates, setPassRates] = useState<PassRateEntry[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Fetch available labs on mount
  useEffect(() => {
    async function fetchLabs() {
      try {
        const data = await fetchWithAuth<Item[]>('/items/')
        const labItems = data.filter(
          (item) => item.type === 'lab' || item.title.toLowerCase().includes('lab'),
        )
        setLabs(labItems.map((item) => ({ id: item.id.toString(), title: item.title })))
        if (labItems.length > 0) {
          const labId = labItems[0].id.toString()
          setSelectedLab(labId)
        }
      } catch {
        // If we can't fetch labs, user can still manually enter or use default
        setLabs([
          { id: 'lab-01', title: 'Lab 01' },
          { id: 'lab-02', title: 'Lab 02' },
          { id: 'lab-03', title: 'Lab 03' },
          { id: 'lab-04', title: 'Lab 04' },
          { id: 'lab-05', title: 'Lab 05' },
        ])
        setSelectedLab('lab-04')
      }
    }
    fetchLabs()
  }, [])

  // Fetch analytics data when lab changes
  useEffect(() => {
    if (!selectedLab) return

    async function fetchAnalytics() {
      setLoading(true)
      setError(null)
      try {
        const [scoresData, timelineData, passRatesData] = await Promise.all([
          fetchWithAuth<ScoreBucket[]>(`/analytics/scores?lab=${selectedLab}`),
          fetchWithAuth<TimelineEntry[]>(`/analytics/timeline?lab=${selectedLab}`),
          fetchWithAuth<PassRateEntry[]>(`/analytics/pass-rates?lab=${selectedLab}`),
        ])
        setScores(scoresData)
        setTimeline(timelineData)
        setPassRates(passRatesData)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error')
      } finally {
        setLoading(false)
      }
    }
    fetchAnalytics()
  }, [selectedLab])

  // Bar chart data for score buckets
  const barChartData = {
    labels: scores.map((s) => s.bucket),
    datasets: [
      {
        label: 'Number of Students',
        data: scores.map((s) => s.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  const barChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: true,
        text: 'Score Distribution',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          stepSize: 1,
        },
      },
    },
  } as const

  // Line chart data for timeline
  const lineChartData = {
    labels: timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.map((t) => t.submissions),
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 2,
        tension: 0.3,
      },
    ],
  }

  const lineChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: true,
        text: 'Submissions Over Time',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          stepSize: 1,
        },
      },
    },
  } as const

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h2>Analytics Dashboard</h2>
        <div className="lab-selector">
          <label htmlFor="lab-select">Select Lab: </label>
          <select
            id="lab-select"
            value={selectedLab}
            onChange={(e) => setSelectedLab(e.target.value)}
          >
            {labs.map((lab) => (
              <option key={lab.id} value={lab.id}>
                {lab.title}
              </option>
            ))}
          </select>
        </div>
      </div>

      {loading && <p className="loading">Loading analytics...</p>}
      {error && <p className="error">Error: {error}</p>}

      {!loading && !error && (
        <div className="dashboard-content">
          <div className="chart-container">
            <Bar data={barChartData} options={barChartOptions} />
          </div>

          <div className="chart-container">
            <Line data={lineChartData} options={lineChartOptions} />
          </div>

          <div className="table-container">
            <h3>Pass Rates per Task</h3>
            {passRates.length === 0 ? (
              <p>No data available</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Avg Score</th>
                    <th>Attempts</th>
                  </tr>
                </thead>
                <tbody>
                  {passRates.map((entry) => (
                    <tr key={entry.task}>
                      <td>{entry.task}</td>
                      <td>{entry.avg_score}</td>
                      <td>{entry.attempts}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
