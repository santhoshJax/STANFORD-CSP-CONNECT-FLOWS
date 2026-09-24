import { componentManifests } from "@prismatic-io/spectral";
import googleDrive from "./manifests/google-drive";
import salesforce from "./manifests/salesforce";

export const componentRegistry = componentManifests({
  googleDrive,
  salesforce,
});
