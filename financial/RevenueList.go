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

type RevenueList struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	Period                   int32
	Year                     int32
	Amount                   float64
}

func getRevenueList(c *financial.RevenueList, organisationID int64, softwareClientLicenceID int64) RevenueList {
	return RevenueList{
		organisationID,
		softwareClientLicenceID,
		c.Period,
		c.Year,
		c.Amount,
	}
}

func (service *Service) WriteRevenueLists(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialService().NewGetRevenueListsCall()

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		revenueLists, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if revenueLists == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *revenueLists {
			batchRowCount++

			b, err := json.Marshal(getRevenueList(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#RevenueLists flushed: %v\n", batchRowCount)

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

	fmt.Printf("#RevenueLists: %v\n", rowCount)

	return objectHandles, rowCount, RevenueList{}, nil
}
