import React, { useState, useEffect } from "react";
import axios from "axios";
import TrendingCard from "./components/TrendingCard";
import "./App.css";

const App = () => {
  const [trendingData, setTrendingData] = useState([]);
  const [selectedDate, setSelectedDate] = useState("");

  const handleDateChange = (event) => {
    setSelectedDate(event.target.value);
  };

  useEffect(() => {
    // Fetch data based on the selected date
    axios
      .get(
        `https://khout2ucs4.execute-api.us-east-1.amazonaws.com/prod/query?date=${selectedDate}`
      )
      .then((response) => {
        setTrendingData(response.data); // Assuming response.data contains an array of items
      })
      .catch((error) => {
        console.error("Error fetching data:", error);
      });
  }, [selectedDate]); // Fetch when date changes

  return (
    <div className="app-container">
      <h1>YouTube Trending Videos In USA</h1>

      <div className="date-picker-container">
        <label htmlFor="date-picker">Select Date:</label>
        <input
          type="date"
          id="date-picker"
          value={selectedDate}
          onChange={handleDateChange}
          className="date-picker"
        />
      </div>
      <div className="cards-container">
        {trendingData.length > 0 ? (
          trendingData.map((item, index) => (
            <TrendingCard key={index} data={item} />
          ))
        ) : (
          <p>No trending data available for this date.</p>
        )}
      </div>
    </div>
  );
};

export default App;
