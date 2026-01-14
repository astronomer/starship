/**
 * Merge local and remote DAG data into a single array
 * @param {Array} localData - Array of local DAG objects
 * @param {Array} remoteData - Array of remote DAG objects
 * @returns {Array} - Array of merged { local, remote } objects
 */
export default function mergeDagData(localData, remoteData) {
  const output = {};

  // Add all local DAGs first (these are the DAGs we can migrate FROM)
  localData.forEach((item) => {
    output[item.dag_id] = { local: item, remote: null };
  });

  // Then merge in remote DAG data where it exists
  remoteData.forEach((item) => {
    if (output[item.dag_id]) {
      output[item.dag_id].remote = item;
    }
  });

  return Object.values(output);
}
