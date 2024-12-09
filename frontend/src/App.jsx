import MapChart from "./components/Map";

function App() {
  return (
    <div className="flex flex-col items-center min-h-screen">
      <h1 className="text-2xl font-bold my-4">Yelp Big Data Analysis</h1>
      <p className="m-10 items-center">Below is an interactive map of the United States. Click on a state to find out more information about its Yelp Reviews, such as the most important attribute for highly rated restaurants (Top Attribute). </p>
      <div className="w-full lg:w-1/2 p-4 bg-white shadow rounded">
        <MapChart />
      </div>
    </div>
  );
}

export default App