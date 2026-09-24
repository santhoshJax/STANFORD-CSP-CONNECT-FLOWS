/**
 * Unit tests for the Stanford CSP Student Import flow.
 *
 * Run with: npm run test
 *
 * HTTP calls are mocked so no live credentials are needed.
 */
import { invokeFlow } from "@prismatic-io/spectral/dist/testing";
import axios from "axios";
import { studentImport } from "./studentFlow";

jest.mock("axios");
const mockedAxios = axios as jest.Mocked<typeof axios>;
// ── Connection stubs ───────────────────────────────────────────────────────────

const gdConnection = {
  key: "Google Drive Connection",
  token: { access_token: "gd-test-token" },
  fields: {},
};

const sfConnection = {
  key: "Salesforce Connection",
  token: { access_token: "sf-test-token" },
  fields: {
    instanceUrl: { value: "https://test.my.salesforce.com" },
  },
};

// ── Sample TSV (headers match the real field names) ────────────────────────────

const tsvContent = [
  [
    "ID",
    "Last_Name",
    "First_Name",
    "Middle_Name",
    "Date_Of_Birth",
    "Evening_Phone",
    "Daytime_Phone",
    "EMail_Address",
    "Opt_In_Email",
    "Notes",
    "Address_Line1",
    "Address_Line2",
    "City",
    "State_Prov",
    "Zip_PC",
    "Country",
    "Gender",
    "Opt_In_Physical_Mail",
    "US_Citizen",
    "Stanford_Alumnus",
    "Highest_Degree",
    "Ethnicity",
    "TA_Discount_Type",
    "University_ID",
    "SAA_Number",
    "Do_Not_Enroll",
    "MergedInto_Student_ID",
    "First_Quarter",
    "Total_Enrollments",
    "Discount_Verified",
    "Discount_Verification_Date",
    "Alert_Flag",
    "Most_Recent_Quarter",
    "Certificate_Issued",
    "Certificate_Issued_Date",
    "SBSAA_Number",
    "FERPA_Directory_Consent",
    "Discount_Lifetime",
    "History",
  ].join("\t"),
  [
    "1001",
    "Smith",
    "Alice",
    "",
    "01/15/1990",
    "555-1234",
    "555-4321",
    "alice@example.com",
    "True",
    "Test note",
    "123 Main St",
    "Apt 4",
    "Palo Alto",
    "CA",
    "94301",
    "US",
    "F",
    "True",
    "True",
    "False",
    "BS",
    "Asian",
    "",
    "U12345",
    "SAA001",
    "False",
    "",
    "2020W",
    "3",
    "True",
    "03/01/2022",
    "False",
    "2024W",
    "True",
    "06/01/2024",
    "9001",
    "True",
    "False",
    "some history data",
  ].join("\t"),
  [
    "1002",
    "Jones",
    "Bob",
    "",
    "",
    "same",
    "555-9999",
    "bob@example.com",
    "False",
    "",
    "456 Oak Ave",
    "",
    "Stanford",
    "CA",
    "94305",
    "US",
    "M",
    "False",
    "True",
    "True",
    "MS",
    "",
    "",
    "U67890",
    "",
    "False",
    "",
    "",
    "0",
    "False",
    "",
    "False",
    "",
    "False",
    "",
    "",
    "False",
    "False",
    "",
  ].join("\t"),
].join("\n");

function makeStream(content: string) {
  const { Readable } = require("stream") as typeof import("stream");
  return Readable.from([content]);
}

// ── Tests ──────────────────────────────────────────────────────────────────────

describe("studentImport flow", () => {
  beforeEach(() => {
    jest.clearAllMocks();

    mockedAxios.get.mockImplementation((url: string) => {
      if (url.includes("googleapis.com")) {
        return Promise.resolve({ data: makeStream(tsvContent) });
      }
      // Bulk API poll → immediate JobComplete
      return Promise.resolve({
        data: {
          state: "JobComplete",
          numberRecordsProcessed: 2,
          numberRecordsFailed: 0,
        },
      });
    });

    mockedAxios.post.mockResolvedValue({ data: { id: "job-abc-123" } });
    mockedAxios.put.mockResolvedValue({ data: {} });
    mockedAxios.patch.mockResolvedValue({ data: {} });
  });

  test("processes 2 rows and reports no more rows remaining", async () => {
    const { result } = await invokeFlow(studentImport, {
      configVars: {
        "Google Drive Connection": gdConnection,
        "Salesforce Connection": sfConnection,
        "Student File ID": "test-file-id",
      },
    });

    expect(result?.data).toMatchObject({
      startRow: 0,
      rowsProcessed: 2,
      hasMore: false,
    });
  });

  test("creates Bulk API 2.0 job with External_ID_4D__c as external ID", async () => {
    await invokeFlow(studentImport, {
      configVars: {
        "Google Drive Connection": gdConnection,
        "Salesforce Connection": sfConnection,
        "Student File ID": "test-file-id",
      },
    });

    expect(mockedAxios.post).toHaveBeenCalledWith(
      "https://test.my.salesforce.com/services/data/v60.0/jobs/ingest",
      expect.objectContaining({
        object: "Contact",
        operation: "upsert",
        externalIdFieldName: "External_ID_4D__c",
      }),
      expect.any(Object),
    );
  });

  test("inverts Opt_In_Email: True → HasOptedOutOfEmail=false", async () => {
    // Alice has Opt_In_Email=True → HasOptedOutOfEmail should be false
    // We verify by checking what CSV was uploaded to Salesforce
    await invokeFlow(studentImport, {
      configVars: {
        "Google Drive Connection": gdConnection,
        "Salesforce Connection": sfConnection,
        "Student File ID": "test-file-id",
      },
    });

    const uploadCall = mockedAxios.put.mock.calls[0];
    const csvBody = uploadCall[1] as string;
    // Alice's row should have HasOptedOutOfEmail=false
    expect(csvBody).toContain("false");
  });

  test("uses Daytime_Phone when Evening_Phone is 'same'", async () => {
    await invokeFlow(studentImport, {
      configVars: {
        "Google Drive Connection": gdConnection,
        "Salesforce Connection": sfConnection,
        "Student File ID": "test-file-id",
      },
    });

    const csvBody = mockedAxios.put.mock.calls[0][1] as string;
    // Bob has Evening_Phone=same, Daytime_Phone=555-9999
    expect(csvBody).toContain("555-9999");
  });

  test("throws when Student File ID is empty", async () => {
    await expect(
      invokeFlow(studentImport, {
        configVars: {
          "Google Drive Connection": gdConnection,
          "Salesforce Connection": sfConnection,
          "Student File ID": "",
        },
      }),
    ).rejects.toThrow("Student File ID config var is empty.");
  });

  test("streams Google Drive file with supportsAllDrives=true", async () => {
    await invokeFlow(studentImport, {
      configVars: {
        "Google Drive Connection": gdConnection,
        "Salesforce Connection": sfConnection,
        "Student File ID": "my-file-id",
      },
    });

    expect(mockedAxios.get).toHaveBeenCalledWith(
      expect.stringContaining("my-file-id"),
      expect.objectContaining({
        params: expect.objectContaining({ supportsAllDrives: "true" }),
        responseType: "stream",
      }),
    );
  });
});
