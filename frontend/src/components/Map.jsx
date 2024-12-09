import React, { useState, useEffect } from "react";
import { ComposableMap, Geographies, Geography, ZoomableGroup } from "react-simple-maps";

import state from "../data/stateInfo.json";
const geoUrl = "src/states-10m.json";

const MapChart = () => {
  const [tooltipContent, setTooltipContent] = useState([]);
  const [selectedState, setSelectedState] = useState(null);

  const handleStateClick = (geo) => {
    const stateId = geo.id;
    const stateInfo = state[stateId];

    setSelectedState(stateId);
    if (stateInfo) {

      setTooltipContent([
        `State 🌎: ${stateInfo.state}`,
        `Cool 😎: ${stateInfo.cool}`,
        `Funny 😂: ${stateInfo.funny}`,
        `Useful 🛠️: ${stateInfo.useful}`,
        `Avg Stars ⭐⭐⭐⭐⭐: ${stateInfo.avgStars.toFixed(2)}`,
        `Top Attribute 🌟: ${stateInfo.attribute}`,
      ]);

    } 
    else {
      setTooltipContent(["Insufficient/no data by Yelp for this state."]);
    }
  };

  useEffect(() => {
    // This query selector is a bit tricky. I don't know what this snippet does.
    const images = document.querySelectorAll(".state-chart-image");
    images.forEach((img) => {
      img.style.display = "";
    });
  }, [selectedState]);

  return (
    <div>
      <ComposableMap
        projection="geoAlbersUsa"
        projectionConfig={{ scale: 500 }}
      >
        <ZoomableGroup center={[-97, 37]} zoom={2}>
          <Geographies geography={geoUrl}>
            {({ geographies }) =>
              geographies.map((geo) => (
                <Geography key={geo.rsmKey} geography={geo}
                  onClick={() => handleStateClick(geo)}
                  style={{
                    default: {
                      fill: selectedState === geo.id ? "#F53" : "#D6D6DA",
                    },
                    hover: {
                      fill: "#F53",
                    },
                    pressed: {
                      fill: "#E42",
                    },
                  }}
                />
              ))
            }
          </Geographies>
        </ZoomableGroup>
      </ComposableMap>

      <div className="mt-6 p-4 bg-red-100 rounded-md flex">
        <div className="flex-1">
          <h3 className="text-lg font-bold text-gray-800 mb-2">State Info:</h3>
          <div>
            {tooltipContent.map((line, index) => (
              <div key={index}>{line}</div>
            ))}
          </div>
        </div>

        {/*images handling */}
        <div className="ml-4 flex flex-col space-y-2">
          {selectedState && (
            <>
              <img
                src={`src/data/charts/${selectedState}.png`}
                className="state-chart-image rounded shadow bg-transparent"
                onError={(e) => (e.target.style.display = "none")}
              />
              <img
                src={`src/data/charts/${selectedState}_1.png`}
                className="state-chart-image rounded shadow bg-transparent"
                onError={(e) => (e.target.style.display = "none")}
              />
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default MapChart;
