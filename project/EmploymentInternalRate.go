package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	project "github.com/leapforce-libraries/go_exactonline_new/project"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type EmploymentInternalRate struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	Created_                 time.Time
	Modified_                time.Time
	ID                       string
	Created                  bigquery.NullTimestamp
	Creator                  string
	CreatorFullName          string
	Division                 int64
	Employee                 string
	EmployeeFullName         string
	EmployeeHID              int64
	Employment               string
	EmploymentHID            int64
	EndDate                  bigquery.NullTimestamp
	InternalRate             float64
	Modified                 bigquery.NullTimestamp
	Modifier                 string
	ModifierFullName         string
	StartDate                bigquery.NullTimestamp
}

func getEmploymentInternalRate(c *project.EmploymentInternalRate, organisationID int64, softwareClientLicenceID int64) EmploymentInternalRate {
	t := time.Now()

	return EmploymentInternalRate{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.ID.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.Employee.String(),
		c.EmployeeFullName,
		c.EmployeeHID,
		c.Employment.String(),
		c.EmploymentHID,
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.InternalRate,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		go_bigquery.DateToNullTimestamp(c.StartDate),
	}
}

func (service *Service) WriteEmploymentInternalRates(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.ProjectService().NewGetEmploymentInternalRatesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		employmentInternalRates, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if employmentInternalRates == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *employmentInternalRates {
			batchRowCount++

			b, err := json.Marshal(getEmploymentInternalRate(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#EmploymentInternalRates flushed: %v\n", batchRowCount)

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

	fmt.Printf("#EmploymentInternalRates: %v\n", rowCount)

	return objectHandles, rowCount, EmploymentInternalRate{}, nil
}
