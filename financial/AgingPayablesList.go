package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financial "github.com/leapforce-libraries/go_exactonline_new/financial"
	types "github.com/leapforce-libraries/go_types"
)

type AgingPayablesList struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	AccountId                  string
	AccountCode                string
	AccountName                string
	AgeGroup1                  int32
	AgeGroup1Amount            float64
	AgeGroup1Description       string
	AgeGroup2                  int32
	AgeGroup2Amount            float64
	AgeGroup2Description       string
	AgeGroup3                  int32
	AgeGroup3Amount            float64
	AgeGroup3Description       string
	AgeGroup4                  int32
	AgeGroup4Amount            float64
	AgeGroup4Description       string
	CurrencyCode               string
	TotalAmount                float64
}

func getAgingPayablesList(c *financial.AgingPayablesList, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) AgingPayablesList {
	t := time.Now()

	return AgingPayablesList{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.AccountId.String(),
		c.AccountCode,
		c.AccountName,
		c.AgeGroup1,
		c.AgeGroup1Amount,
		c.AgeGroup1Description,
		c.AgeGroup2,
		c.AgeGroup2Amount,
		c.AgeGroup2Description,
		c.AgeGroup3,
		c.AgeGroup3Amount,
		c.AgeGroup3Description,
		c.AgeGroup4,
		c.AgeGroup4Amount,
		c.AgeGroup4Description,
		c.CurrencyCode,
		c.TotalAmount,
	}
}

func (service *Service) WriteAgingPayablesLists(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialService().NewGetAgingPayablesListsCall()

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for {
		agingPayablesLists, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if agingPayablesLists == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *agingPayablesLists {
			batchRowCount++

			b, err := json.Marshal(getAgingPayablesList(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
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

			fmt.Printf("#AgingPayablesLists flushed: %v\n", batchRowCount)

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

	fmt.Printf("#AgingPayablesLists: %v\n", rowCount)

	return objectHandles, rowCount, AgingPayablesList{}, nil
}
