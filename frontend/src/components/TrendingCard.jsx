import React from "react";
import "./TrendingCard.css";

const TrendingCard = ({ data }) => {
  return (
    <a
      href={`https://www.youtube.com/watch?v=${data.video_id}`}
      target="_blank"
      rel="noopener noreferrer"
      className="card-link"
    >
      <div className="card">
        <img src={data.thumbnails} alt="Thumbnail" className="card-img" />
        <div className="card-content">
          <h3 className="card-title">{data.title}</h3>
          <p className="card-channel">Channel: {data.channel_title}</p>
          <p className="card-published">
            Published at: {new Date(data.published_at).toLocaleString()}
          </p>
          <p className="card-stats">
            Views: {data.view_count} | Likes: {data.like_count} | Comments:{" "}
            {data.comment_count}
          </p>
        </div>
      </div>
    </a>
  );
};

export default TrendingCard;
