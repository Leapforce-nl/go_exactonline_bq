package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	_bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	crm "github.com/leapforce-libraries/go_exactonline_new/crm"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type ContactBQ struct {
	ClientID                  string
	ID                        string
	Account                   string
	AccountIsCustomer         bool
	AccountIsSupplier         bool
	AccountMainContact        string
	AccountName               string
	AddressLine2              string
	AddressStreet             string
	AddressStreetNumber       string
	AddressStreetNumberSuffix string
	AllowMailing              int32
	BirthDate                 _bigquery.NullTimestamp
	BirthName                 string
	BirthNamePrefix           string
	BirthPlace                string
	BusinessEmail             string
	BusinessFax               string
	BusinessMobile            string
	BusinessPhone             string
	BusinessPhoneExtension    string
	City                      string
	Code                      string
	Country                   string
	Created                   _bigquery.NullTimestamp
	Creator                   string
	CreatorFullName           string
	Division                  int32
	Email                     string
	EndDate                   _bigquery.NullTimestamp
	FirstName                 string
	FullName                  string
	Gender                    string
	HID                       int32
	IdentificationDate        _bigquery.NullTimestamp
	IdentificationDocument    string
	IdentificationUser        string
	Initials                  string
	IsAnonymised              byte
	IsMailingExcluded         bool
	IsMainContact             bool
	JobTitleDescription       string
	Language                  string
	LastName                  string
	LeadPurpose               string
	LeadSource                string
	MarketingNotes            string
	MiddleName                string
	Mobile                    string
	Modified                  _bigquery.NullTimestamp
	Modifier                  string
	ModifierFullName          string
	Nationality               string
	Notes                     string
	PartnerName               string
	PartnerNamePrefix         string
	Person                    string
	Phone                     string
	PhoneExtension            string
	PictureName               string
	PictureThumbnailUrl       string
	PictureUrl                string
	Postcode                  string
	SocialSecurityNumber      string
	StartDate                 _bigquery.NullTimestamp
	State                     string
	Title                     string
}

func getContactBQ(c *crm.Contact, clientID string) ContactBQ {
	return ContactBQ{
		clientID,
		c.ID.String(),
		c.Account.String(),
		c.AccountIsCustomer,
		c.AccountIsSupplier,
		c.AccountMainContact.String(),
		c.AccountName,
		c.AddressLine2,
		c.AddressStreet,
		c.AddressStreetNumber,
		c.AddressStreetNumberSuffix,
		c.AllowMailing,
		bigquery.DateToNullTimestamp(c.BirthDate),
		c.BirthName,
		c.BirthNamePrefix,
		c.BirthPlace,
		c.BusinessEmail,
		c.BusinessFax,
		c.BusinessMobile,
		c.BusinessPhone,
		c.BusinessPhoneExtension,
		c.City,
		c.Code,
		c.Country,
		bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.Email,
		bigquery.DateToNullTimestamp(c.EndDate),
		c.FirstName,
		c.FullName,
		c.Gender,
		c.HID,
		bigquery.DateToNullTimestamp(c.IdentificationDate),
		c.IdentificationDocument.String(),
		c.IdentificationUser.String(),
		c.Initials,
		c.IsAnonymised,
		c.IsMailingExcluded,
		c.IsMainContact,
		c.JobTitleDescription,
		c.Language,
		c.LastName,
		c.LeadPurpose.String(),
		c.LeadSource.String(),
		c.MarketingNotes,
		c.MiddleName,
		c.Mobile,
		bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Nationality,
		c.Notes,
		c.PartnerName,
		c.PartnerNamePrefix,
		c.Person.String(),
		c.Phone,
		c.PhoneExtension,
		c.PictureName,
		c.PictureThumbnailUrl,
		c.PictureUrl,
		c.Postcode,
		c.SocialSecurityNumber,
		bigquery.DateToNullTimestamp(c.StartDate),
		c.State,
		c.Title,
	}
}

func (service *Service) WriteContactsBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	getContactsCallParams := crm.GetContactsCallParams{
		ModifiedAfter: lastModified,
	}

	call := service.CRMService().NewGetContactsCall(&getContactsCallParams)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		contacts, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if contacts == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *contacts {
			batchRowCount++

			b, err := json.Marshal(getContactBQ(&tl, service.ClientID()))
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

			fmt.Printf("#Contacts for service %s flushed: %v\n", service.ClientID(), batchRowCount)

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

	fmt.Printf("#Contacts for client %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, ContactBQ{}, nil
}
