package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	payroll "github.com/leapforce-libraries/go_exactonline_new/payroll"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type EmploymentContract struct {
	OrganisationID_              int64
	SoftwareClientLicenceID_     int64
	Created_                     time.Time
	Modified_                    time.Time
	ID                           string
	ContractFlexPhase            int32
	ContractFlexPhaseDescription string
	Created                      bigquery.NullTimestamp
	Creator                      string
	CreatorFullName              string
	Division                     int32
	Document                     string
	Employee                     string
	EmployeeFullName             string
	EmployeeHID                  int32
	EmployeeType                 int32
	EmployeeTypeDescription      string
	Employment                   string
	EmploymentHID                int32
	EndDate                      bigquery.NullTimestamp
	Modified                     bigquery.NullTimestamp
	Modifier                     string
	ModifierFullName             string
	Notes                        string
	ProbationEndDate             bigquery.NullTimestamp
	ProbationPeriod              int32
	ReasonContract               int32
	ReasonContractDescription    string
	Sequence                     int32
	StartDate                    bigquery.NullTimestamp
	Type                         int32
	TypeDescription              string
}

func getEmploymentContract(c *payroll.EmploymentContract, organisationID int64, softwareClientLicenceID int64) EmploymentContract {
	t := time.Now()

	return EmploymentContract{
		organisationID,
		softwareClientLicenceID,
		t,
		t,
		c.ID.String(),
		c.ContractFlexPhase,
		c.ContractFlexPhaseDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.Document.String(),
		c.Employee.String(),
		c.EmployeeFullName,
		c.EmployeeHID,
		c.EmployeeType,
		c.EmployeeTypeDescription,
		c.Employment.String(),
		c.EmploymentHID,
		go_bigquery.DateToNullTimestamp(c.EndDate),
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		go_bigquery.DateToNullTimestamp(c.ProbationEndDate),
		c.ProbationPeriod,
		c.ReasonContract,
		c.ReasonContractDescription,
		c.Sequence,
		go_bigquery.DateToNullTimestamp(c.StartDate),
		c.Type,
		c.TypeDescription,
	}
}

func (service *Service) WriteEmploymentContracts(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PayrollService().NewGetEmploymentContractsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		employmentContracts, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if employmentContracts == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *employmentContracts {
			batchRowCount++

			b, err := json.Marshal(getEmploymentContract(&tl, organisationID, softwareClientLicenceID))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#EmploymentContracts flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#EmploymentContracts: %v\n", rowCount)

	return objectHandles, rowCount, EmploymentContract{}, nil
}
